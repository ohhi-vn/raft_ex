defmodule RaftEx.Server.Election do
  @moduledoc """
  Raft leader election implementation with pre-vote support.

  This module handles the election process including:
  - Pre-vote phase: Checks if an election would succeed before incrementing term
  - Candidate phase: Actual voting with term increment
  - Vote request/reply handling
  - Quorum evaluation
  - Election timeout management

  ## Election Flow

  1. **Pre-Vote**: Candidate probes peers without incrementing term
  2. **Vote Request**: If pre-vote succeeds, increment term and request votes
  3. **Vote Reply**: Peers grant vote if candidate's log is up-to-date
  4. **Win Condition**: Candidate wins if it receives votes from majority

  ## Pre-Vote Benefits

  Pre-vote prevents a partitioned node from disrupting the cluster by:
  - Not incrementing its term until it knows it can win
  - Avoiding unnecessary term increases that would force leader re-election
  - Allowing the node to catch up before starting a real election
  """

  require Logger

  alias RaftEx.Types
  alias RaftEx.Server.Config

  @type election_state :: :pre_vote | :candidate
  @type election_result :: :won | :lost | :ongoing

  @type election_context :: %{
          state: election_state(),
          term: Types.term_num(),
          votes_received: MapSet.t(Types.server_id()),
          total_voters: non_neg_integer(),
          last_log_index: Types.index(),
          last_log_term: Types.term_num()
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Start an election (pre-vote or candidate phase).

  Returns `{election_context, effects}` where effects include RPCs to send to peers.
  """
  @spec start_election(election_state(), map()) :: {election_context(), [term()]}
  def start_election(:pre_vote = state, %{
        cfg: %Config{id: id, uid: _uid},
        current_term: term,
        log: log,
        cluster: cluster
      }) do
    {last_log_index, last_log_term} = RaftEx.Log.last_index_term(log)

    context = %{
      state: state,
      term: term,
      votes_received: MapSet.new([id]),
      total_voters: count_voters(cluster),
      last_log_index: last_log_index,
      last_log_term: last_log_term
    }

    # Build pre-vote RPC
    rpc = %Types.PreVoteRpc{
      machine_version: 0,
      term: term,
      token: generate_token(),
      candidate_id: id,
      last_log_index: last_log_index,
      last_log_term: last_log_term,
      version: 1
    }

    effects = build_election_rpcs(cluster, id, rpc, :pre_vote)

    Logger.info(
      "RaftEx.Election: Starting pre-vote for term #{term}, " <>
        "last_log_index=#{last_log_index}, last_log_term=#{last_log_term}"
    )

    {context, effects}
  end

  def start_election(:candidate = state, %{
        cfg: %Config{id: id, uid: _uid, log_id: _log_id},
        current_term: term,
        log: log,
        cluster: cluster
      }) do
    # Increment term for candidate phase
    new_term = term + 1

    {last_log_index, last_log_term} = RaftEx.Log.last_index_term(log)

    context = %{
      state: state,
      term: new_term,
      votes_received: MapSet.new([id]),
      total_voters: count_voters(cluster),
      last_log_index: last_log_index,
      last_log_term: last_log_term
    }

    # Update our vote to ourselves
    effects = [{:store_meta, :current_term, new_term}, {:store_meta, :voted_for, id}]

    # Build vote request RPC
    rpc = %Types.RequestVoteRpc{
      term: new_term,
      candidate_id: id,
      last_log_index: last_log_index,
      last_log_term: last_log_term
    }

    vote_effects = build_election_rpcs(cluster, id, rpc, :vote)

    Logger.info(
      "RaftEx.Election: Starting candidate election for term #{new_term}, " <>
        "last_log_index=#{last_log_index}, last_log_term=#{last_log_term}"
    )

    {context, effects ++ vote_effects}
  end

  @doc """
  Handle an incoming pre-vote request.

  Returns `{vote_granted, effects}`.
  """
  @spec handle_pre_vote_request(Types.PreVoteRpc.t(), map()) :: {boolean(), [term()]}
  def handle_pre_vote_request(
        %Types.PreVoteRpc{
          term: candidate_term,
          candidate_id: candidate_id,
          last_log_index: candidate_last_index,
          last_log_term: candidate_last_term
        },
        %{
          cfg: %Config{id: _id, uid: _uid},
          current_term: current_term,
          log: log
        } = _state
      ) do
    {last_log_index, last_log_term} = RaftEx.Log.last_index_term(log)

    # Grant pre-vote if:
    # 1. Candidate's term is >= our current term
    # 2. Candidate's log is at least as up-to-date as ours
    term_ok = candidate_term >= current_term

    log_ok =
      log_is_up_to_date?(candidate_last_index, candidate_last_term, last_log_index, last_log_term)

    vote_granted = term_ok and log_ok

    if vote_granted do
      Logger.debug(
        "RaftEx.Election: Pre-vote granted to #{inspect(candidate_id)} for term #{candidate_term}"
      )
    else
      Logger.debug(
        "RaftEx.Election: Pre-vote denied to #{inspect(candidate_id)} " <>
          "(term_ok=#{term_ok}, log_ok=#{log_ok})"
      )
    end

    {vote_granted, []}
  end

  @doc """
  Handle an incoming vote request.

  Returns `{vote_granted, updated_state, effects}`.
  """
  @spec handle_vote_request(Types.RequestVoteRpc.t(), map()) :: {boolean(), map(), [term()]}
  def handle_vote_request(
        %Types.RequestVoteRpc{
          term: candidate_term,
          candidate_id: candidate_id,
          last_log_index: candidate_last_index,
          last_log_term: candidate_last_term
        },
        %{
          cfg: %Config{id: _id, uid: uid},
          current_term: current_term,
          log: log
        } = state
      ) do
    {last_log_index, last_log_term} = RaftEx.Log.last_index_term(log)

    # Determine if we should grant the vote
    {vote_granted, new_state} =
      cond do
        # Candidate's term is less than ours - deny
        candidate_term < current_term ->
          {false, state}

        # Candidate's term is greater - check log first, then update term if granting vote
        candidate_term > current_term ->
          log_ok =
            log_is_up_to_date?(
              candidate_last_index,
              candidate_last_term,
              last_log_index,
              last_log_term
            )

          if log_ok do
            # Only update term and voted_for if we're actually granting the vote
            new_state = update_term_and_voted_for(candidate_term, candidate_id, state)
            {true, new_state}
          else
            # Deny vote without updating term - prevents election livelock
            {false, state}
          end

        # Same term - check if we already voted
        candidate_term == current_term ->
          voted_for =
            RaftEx.LogMeta.fetch(state.cfg.system_config.names.log_meta, uid, :voted_for)

          cond do
            # Already voted for someone else - deny
            voted_for != nil and voted_for != candidate_id ->
              {false, state}

            # Haven't voted or already voted for this candidate - check log first
            true ->
              log_ok =
                log_is_up_to_date?(
                  candidate_last_index,
                  candidate_last_term,
                  last_log_index,
                  last_log_term
                )

              if log_ok do
                new_state = update_term_and_voted_for(candidate_term, candidate_id, state)
                {true, new_state}
              else
                {false, state}
              end
          end
      end

    effects =
      if vote_granted do
        [
          {:store_meta, :current_term, new_state.current_term},
          {:store_meta, :voted_for, candidate_id}
        ]
      else
        []
      end

    if vote_granted do
      Logger.debug(
        "RaftEx.Election: Vote granted to #{inspect(candidate_id)} for term #{candidate_term}"
      )
    else
      Logger.debug(
        "RaftEx.Election: Vote denied to #{inspect(candidate_id)} for term #{candidate_term}"
      )
    end

    {vote_granted, new_state, effects}
  end

  @doc """
  Handle a vote reply from a peer.

  Returns `{election_result, updated_context, effects}`.
  """
  @spec handle_vote_reply(
          Types.RequestVoteResult.t(),
          election_context(),
          map(),
          RaftEx.Types.server_id()
        ) ::
          {election_result(), election_context(), [term()]}
  def handle_vote_reply(
        %Types.RequestVoteResult{term: term, vote_granted: true},
        %{state: :candidate, term: election_term, votes_received: votes, total_voters: total} =
          context,
        _state,
        peer_id
      )
      when term == election_term do
    # Add vote from the peer who replied
    new_votes = MapSet.put(votes, peer_id)
    new_context = %{context | votes_received: new_votes}

    # Check if we won
    if MapSet.size(new_votes) > div(total, 2) do
      Logger.info(
        "RaftEx.Election: Won election for term #{election_term} with #{MapSet.size(new_votes)}/#{total} votes"
      )

      {:won, new_context, [{:become, :leader}]}
    else
      {:ongoing, new_context, []}
    end
  end

  def handle_vote_reply(
        %Types.RequestVoteResult{term: term, vote_granted: false},
        %{state: :candidate, term: election_term} = context,
        _state,
        _peer_id
      )
      when term > election_term do
    # Peer has higher term - step down
    Logger.info("RaftEx.Election: Stepping down, peer has higher term #{term} > #{election_term}")

    {:lost, context, [{:become, :follower}, {:update_term, term}]}
  end

  def handle_vote_reply(
        %Types.RequestVoteResult{vote_granted: false},
        context,
        _state,
        _peer_id
      ) do
    # Vote denied but term is OK - continue waiting
    {:ongoing, context, []}
  end

  @doc """
  Handle a pre-vote reply from a peer.

  Returns `{should_start_election, updated_context, effects}`.
  """
  @spec handle_pre_vote_reply(
          Types.PreVoteResult.t(),
          election_context(),
          map(),
          RaftEx.Types.server_id()
        ) ::
          {boolean(), election_context(), [term()]}
  def handle_pre_vote_reply(
        %Types.PreVoteResult{term: term, vote_granted: true},
        %{state: :pre_vote, term: election_term, votes_received: votes, total_voters: total} =
          context,
        _state,
        peer_id
      )
      when term == election_term do
    # Add vote from the peer who replied
    new_votes = MapSet.put(votes, peer_id)
    new_context = %{context | votes_received: new_votes}

    # Check if we have enough pre-votes to start real election
    if MapSet.size(new_votes) > div(total, 2) do
      Logger.info(
        "RaftEx.Election: Pre-vote succeeded with #{MapSet.size(new_votes)}/#{total} votes, starting candidate election"
      )

      {true, new_context, []}
    else
      {false, new_context, []}
    end
  end

  def handle_pre_vote_reply(
        %Types.PreVoteResult{term: term},
        %{state: :pre_vote, term: election_term} = context,
        _state,
        _peer_id
      )
      when term > election_term do
    # Peer has higher term - update and step down
    Logger.info(
      "RaftEx.Election: Pre-vote failed, peer has higher term #{term} > #{election_term}"
    )

    {false, context, [{:become, :follower}, {:update_term, term}]}
  end

  def handle_pre_vote_reply(%Types.PreVoteResult{vote_granted: false}, context, _state, _peer_id) do
    # Pre-vote denied
    {false, context, []}
  end

  @doc """
  Evaluate election result based on current votes.

  Returns `:won`, `:lost`, or `:ongoing`.
  """
  @spec evaluate_election_result(election_context()) :: election_result()
  def evaluate_election_result(%{
        votes_received: votes,
        total_voters: total
      }) do
    vote_count = MapSet.size(votes)
    majority = div(total, 2) + 1

    cond do
      vote_count >= majority -> :won
      vote_count < majority and can_still_win?(vote_count, total) -> :ongoing
      true -> :lost
    end
  end

  @doc """
  Check if it's still possible to win the election.

  Returns true if there are enough outstanding votes to reach majority.
  """
  @spec can_still_win?(non_neg_integer(), non_neg_integer()) :: boolean()
  def can_still_win?(votes_received, total_voters) do
    # We can still win if we haven't received replies from all peers yet
    # and it's mathematically possible to reach majority
    majority = div(total_voters, 2) + 1
    votes_received < majority
  end

  @doc """
  Get the number of votes needed to win.

  Returns the majority count.
  """
  @spec votes_needed(non_neg_integer()) :: non_neg_integer()
  def votes_needed(total_voters) do
    div(total_voters, 2) + 1
  end

  # ---------------------------------------------------------------------------
  # Private Helpers
  # ---------------------------------------------------------------------------

  defp count_voters(cluster) do
    Enum.count(cluster, fn {_id, peer} ->
      case peer do
        %{voter_status: %{membership: :voter}} -> true
        %{voter_status: %{membership: :promotable}} -> true
        _ -> false
      end
    end)
  end

  defp build_election_rpcs(cluster, self_id, rpc, _type) do
    cluster
    |> Map.delete(self_id)
    |> Enum.flat_map(fn {peer_id, peer_state} ->
      case peer_state do
        %{voter_status: %{membership: membership}} when membership in [:voter, :promotable] ->
          [{:send_rpc, peer_id, rpc}]

        _ ->
          []
      end
    end)
  end

  defp log_is_up_to_date?(
         candidate_last_index,
         candidate_last_term,
         our_last_index,
         our_last_term
       ) do
    # Candidate's log is up-to-date if:
    # 1. Its last term is greater than ours, OR
    # 2. Last terms are equal and its last index is >= ours
    candidate_last_term > our_last_term or
      (candidate_last_term == our_last_term and candidate_last_index >= our_last_index)
  end

  defp update_term_and_voted_for(term, _voted_for, state) do
    %{state | current_term: term}
  end

  defp generate_token do
    :crypto.strong_rand_bytes(16) |> Base.url_encode64()
  end
end
