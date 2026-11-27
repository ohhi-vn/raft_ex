defmodule RaftEx.Domain.Server.RpcHandler do
  @moduledoc """
  RPC handler for Raft consensus protocol messages.

  This module handles:
  - AppendEntries RPC and replies (log replication and heartbeats)
  - InstallSnapshot RPC and replies (snapshot transfer)
  - Heartbeat RPC and replies (consistent query support)
  - Log consistency checks and conflict resolution

  ## AppendEntries Flow

  1. Leader sends AppendEntries with prev_log_index, prev_log_term, and entries
  2. Follower checks log consistency at prev_log_index
  3. If consistent, follower appends entries and replies with success
  4. If inconsistent, follower replies with failure and next_index hint
  5. Leader updates match_index and next_index for the peer

  ## InstallSnapshot Flow

  1. Leader sends snapshot chunks when follower is too far behind
  2. Follower receives chunks and writes snapshot
  3. On completion, follower installs snapshot and truncates log
  4. Follower replies with last_index and last_term from snapshot

  ## Heartbeat Flow

  1. Leader sends lightweight heartbeats for consistent queries
  2. Follower acknowledges with query_index
  3. Leader collects acknowledgements from majority
  4. Once majority acknowledges, query is safe to execute
  """

  require Logger

  alias RaftEx.Domain.Machine
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.{Cluster, Config}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Filesystem
  alias RaftEx.Infra.Observability.{Counters, Metrics}
  alias RaftEx.Infra.Persistence.{Log, LogMeta, LogSnapshot}

  # ---------------------------------------------------------------------------
  # AppendEntries RPC Handling
  # ---------------------------------------------------------------------------

  @doc """
  Handle an incoming AppendEntries RPC from the leader.

  Returns `{next_state, updated_state, effects}`.
  """
  @spec handle_append_entries_rpc(Types.AppendEntriesRpc.t(), map()) ::
          {atom(), map(), [term()]}
  def handle_append_entries_rpc(
        %Types.AppendEntriesRpc{
          term: leader_term,
          leader_id: leader_id,
          prev_log_index: prev_log_index,
          prev_log_term: prev_log_term,
          entries: entries,
          leader_commit: leader_commit
        },
        %{cfg: %Config{id: _id}, current_term: current_term, log: _log0} = state
      ) do
    cond do
      # Leader's term is behind ours - reject
      leader_term < current_term ->
        reply = build_append_entries_reply(current_term, false, state)
        effects = [{:send_rpc, leader_id, reply}]
        {:follower, state, effects}

      # Leader's term is ahead - update term and become follower
      leader_term > current_term ->
        state = update_term(state, leader_term)

        handle_append_entries_after_term_update(
          leader_id,
          prev_log_index,
          prev_log_term,
          leader_commit,
          entries,
          state
        )

      # Same term - process normally
      leader_term == current_term ->
        handle_append_entries_after_term_update(
          leader_id,
          prev_log_index,
          prev_log_term,
          leader_commit,
          entries,
          state
        )
    end
  end

  defp handle_append_entries_after_term_update(
         leader_id,
         prev_log_index,
         prev_log_term,
         leader_commit,
         entries,
         %{log: _log0, cfg: %Config{id: _id}} = state
       ) do
    # Update leader info
    state = %{state | leader_id: leader_id}

    # Check log consistency
    {log_consistent, state} = check_log_consistency(prev_log_index, prev_log_term, state)

    if log_consistent do
      # Append new entries if any
      {log, append_effects} = append_entries(entries, state.log, state)
      state = %{state | log: log}

      if Enum.any?(append_effects, &match?({:append_failed, _}, &1)) do
        # Durability failure: keep the old log and reject so the leader
        # retries instead of believing the entries are stored.
        {next_index, state} = find_next_index(state)

        reply =
          build_append_entries_reply(state.current_term, false, state, nil, nil, next_index)

        {:follower, state, [{:send_rpc, leader_id, reply}]}
      else
        # Update commit index
        {state, commit_effects} = update_commit_index(leader_commit, state)

        # Build success reply
        {last_index, last_term} = Log.last_index_term(log)
        reply = build_append_entries_reply(state.current_term, true, state, last_index, last_term)

        effects =
          [{:send_rpc, leader_id, reply}, {:reset_election_timeout}] ++
            append_effects ++ commit_effects

        {:follower, state, effects}
      end
    else
      # Log inconsistency - reject
      {next_index, state} = find_next_index(state)

      reply =
        build_append_entries_reply(state.current_term, false, state, nil, nil, next_index)

      effects = [{:send_rpc, leader_id, reply}]
      {:follower, state, effects}
    end
  end

  @doc """
  Handle an AppendEntries reply from a follower.

  Returns `{updated_state, effects}`.
  """
  @spec handle_append_entries_reply(Types.AppendEntriesReply.t(), map()) :: {map(), [term()]}
  def handle_append_entries_reply(
        %Types.AppendEntriesReply{
          term: reply_term,
          success: true,
          next_index: next_index,
          last_index: last_index,
          last_term: _last_term
        },
        %{cfg: %Config{id: _id}, current_term: current_term, cluster: cluster0} = state
      ) do
    cond do
      # Reply term is ahead - step down
      reply_term > current_term ->
        state = update_term(state, reply_term)
        effects = [{:become, :follower}]
        {state, effects}

      # Success - update peer state
      reply_term == current_term ->
        peer_id = state.rpc_from

        if is_nil(peer_id) or not is_map_key(cluster0, peer_id) do
          # Stale reply from an unknown or removed peer: acknowledge nothing
          # and, crucially, do not re-add it to the membership.
          {state, []}
        else
          now_ms = System.monotonic_time(:millisecond)

          Metrics.inc(:append_entries_acked)

          lag =
            case state do
              %{log: log} ->
                {li, _} = Log.last_index_term(log)
                max(li - last_index, 0)

              _ ->
                0
            end

          Metrics.set({:replication_lag, peer_id}, lag)

          peer0 = Map.get(cluster0, peer_id, %{})
          in_flight = Map.get(peer0, :in_flight, :queue.new())
          acked0 = Map.get(peer0, :acked_in_flight, MapSet.new())
          acked0 = if is_struct(acked0, MapSet), do: acked0, else: MapSet.new(acked0)

          # Track this acknowledgement; the set is pruned to the in-flight
          # window below so long-lived peers stay bounded (9.3).
          acked = MapSet.put(acked0, last_index)

          {match_index, new_in_flight, acked} =
            advance_match(Map.get(peer0, :match_index, 0), in_flight, acked, last_index)

          # Clamp the follower's hint: it may only confirm what the leader
          # actually holds, never reach past the leader's log (9.8).
          leader_next =
            case state do
              %{log: log} -> Log.next_index(log)
              _ -> match_index + 1
            end

          hinted =
            if is_integer(next_index) and next_index > 0 do
              min(max(next_index, match_index + 1), leader_next)
            else
              min(match_index + 1, leader_next)
            end

          cluster =
            Cluster.update_peer(
              peer_id,
              %{
                match_index: match_index,
                next_index: hinted,
                status: :normal,
                in_flight: new_in_flight,
                acked_in_flight: acked,
                last_ack: now_ms
              },
              cluster0
            )

          state = %{state | cluster: cluster}

          # Try to advance commit index
          {state, effects} = maybe_advance_commit_index(state)

          # An acknowledgement that still leaves the peer behind (or freed
          # pipeline capacity) schedules another replication pass so a
          # lagging follower catches up without waiting for client
          # traffic or the next tick (9.2).
          peer1 = Map.get(state.cluster, peer_id, %{})
          next_ni = Map.get(peer1, :next_index, hinted)

          effects =
            if next_ni < leader_next do
              effects ++ [{:next_event, :info, :pipeline_rpcs}]
            else
              effects
            end

          {state, effects}
        end

      # Reply term is behind - ignore
      reply_term < current_term ->
        {state, []}
    end
  end

  def handle_append_entries_reply(
        %Types.AppendEntriesReply{
          term: reply_term,
          success: false,
          next_index: next_index
        },
        %{current_term: current_term} = state
      ) do
    cond do
      # Reply term is ahead - step down
      reply_term > current_term ->
        state = update_term(state, reply_term)
        effects = [{:become, :follower}]
        {state, effects}

      # Failure - decrement next_index and retry
      reply_term == current_term ->
        peer_id = state.rpc_from

        if is_nil(peer_id) or not is_map_key(state.cluster, peer_id) do
          # Stale reply from an unknown or removed peer: ignore it instead
          # of crashing on a missing peer or re-adding it to the cluster.
          {state, []}
        else
          now_ms = System.monotonic_time(:millisecond)

          # Backtracking hints only ever move next_index backward: clamp
          # the follower's hint to the current value and ignore hints at
          # or above it; with no usable hint, step back by one (9.8).
          current_ni = Map.get(state.cluster[peer_id], :next_index, 1)

          new_next_index =
            cond do
              is_integer(next_index) and next_index > 0 and next_index < current_ni ->
                next_index

              is_integer(next_index) and next_index > 0 ->
                current_ni

              true ->
                max(1, current_ni - 1)
            end

          cluster =
            Cluster.update_peer(
              peer_id,
              %{
                next_index: new_next_index,
                status: :normal,
                in_flight: :queue.new(),
                acked_in_flight: MapSet.new(),
                last_ack: now_ms
              },
              state.cluster
            )

          state = %{state | cluster: cluster}

          # Schedule retry
          {state, [{:send_append_entries, peer_id}]}
        end

      # Reply term is behind - ignore
      reply_term < current_term ->
        {state, []}
    end
  end

  # ---------------------------------------------------------------------------
  # InstallSnapshot RPC Handling
  # ---------------------------------------------------------------------------

  @doc """
  Handle an incoming InstallSnapshot RPC from the leader.

  Returns `{next_state, updated_state, effects}`.
  """
  @spec handle_install_snapshot_rpc(Types.InstallSnapshotRpc.t(), map()) ::
          {atom(), map(), [term()]}
  def handle_install_snapshot_rpc(
        %Types.InstallSnapshotRpc{
          term: leader_term,
          leader_id: leader_id,
          meta: meta,
          chunk_state: chunk_state,
          data: data
        },
        %{current_term: current_term, log: _log0} = state
      ) do
    cond do
      # Leader's term is behind - reject
      leader_term < current_term ->
        reply = build_install_snapshot_reply(current_term, state, 0, 0)

        # Never delete the last valid snapshot: only discard an in-flight
        # partial transfer, if any.
        state =
          case Map.get(state, :snapshot_state) do
            accept when is_tuple(accept) ->
              LogSnapshot.abort(accept)
              state |> Map.put(:snapshot_state, nil) |> Map.put(:snapshot_meta, nil)

            _ ->
              state
          end

        effects = [{:send_rpc, leader_id, reply}]
        {:follower, state, effects}

      # Leader's term is ahead - adopt it, then process snapshot
      leader_term > current_term ->
        state = adopt_term(state, leader_term)
        state = %{state | leader_id: leader_id}

        case chunk_state do
          :first ->
            handle_first_snapshot_chunk(meta, data, leader_id, state)

          :middle ->
            handle_middle_snapshot_chunk(data, leader_id, state)

          :last ->
            handle_last_snapshot_chunk(data, leader_id, state)

          :full ->
            handle_full_snapshot(meta, data, leader_id, state)
        end

      # Same term - observe leader without touching term/vote
      leader_term == current_term ->
        state = observe_term(state, leader_term)
        state = %{state | leader_id: leader_id}

        case chunk_state do
          :first ->
            handle_first_snapshot_chunk(meta, data, leader_id, state)

          :middle ->
            handle_middle_snapshot_chunk(data, leader_id, state)

          :last ->
            handle_last_snapshot_chunk(data, leader_id, state)

          :full ->
            handle_full_snapshot(meta, data, leader_id, state)
        end
    end
  end

  defp handle_first_snapshot_chunk(meta, data, leader_id, state) do
    Metrics.inc(:snapshot_chunks_received)
    snap_dir = snapshot_dir(state)

    # Close any in-flight partial transfer being superseded. Only the
    # temporary file is removed; a previously installed snapshot survives.
    case Map.get(state, :snapshot_state) do
      prev when is_tuple(prev) ->
        LogSnapshot.abort(prev)

      _ ->
        :ok
    end

    with :ok <- Filesystem.make_dir(snap_dir),
         {:ok, accept_state} <- LogSnapshot.begin_accept(snap_dir, meta),
         {:ok, new_snapshot_state} <-
           LogSnapshot.accept_chunk(data, accept_state) do
      state =
        state
        |> Map.put(:snapshot_state, new_snapshot_state)
        |> Map.put(:snapshot_meta, meta)

      {:receive_snapshot, state, []}
    else
      err -> snapshot_failure(leader_id, state, err)
    end
  end

  defp handle_middle_snapshot_chunk(data, leader_id, %{snapshot_state: accept_state} = state)
       when is_tuple(accept_state) do
    Metrics.inc(:snapshot_chunks_received)

    case LogSnapshot.accept_chunk(data, accept_state) do
      {:ok, new_snapshot_state} ->
        {:receive_snapshot, %{state | snapshot_state: new_snapshot_state}, []}

      {:error, reason} ->
        snapshot_failure(leader_id, state, reason)
    end
  end

  defp handle_middle_snapshot_chunk(_data, leader_id, state) do
    snapshot_failure(leader_id, state, :no_snapshot_in_progress)
  end

  defp handle_last_snapshot_chunk(data, leader_id, %{snapshot_state: accept_state} = state)
       when is_tuple(accept_state) do
    Metrics.inc(:snapshot_chunks_received)
    snap_dir = snapshot_dir(state)

    # Framing (`<<crc::32, payload>>`) is stripped uniformly inside
    # `complete_accept/2`; pass the wire bytes through unchanged.
    case LogSnapshot.complete_accept(data, accept_state) do
      {:ok, _bytes} ->
        case LogSnapshot.recover(snap_dir) do
          {:ok, meta, mac_state} ->
            install_snapshot(meta, mac_state, leader_id, state)

          {:error, reason} ->
            snapshot_failure(leader_id, state, reason)
        end

      {:error, reason} ->
        snapshot_failure(leader_id, state, reason)
    end
  end

  defp handle_last_snapshot_chunk(_data, leader_id, state) do
    snapshot_failure(leader_id, state, :no_snapshot_in_progress)
  end

  defp handle_full_snapshot(meta, data, leader_id, state) do
    Metrics.inc(:snapshot_chunks_received)
    snap_dir = snapshot_dir(state)

    with :ok <- Filesystem.make_dir(snap_dir),
         {:ok, _bytes} <- LogSnapshot.write(snap_dir, meta, data, true),
         {:ok, recovered_meta, mac_state} <-
           LogSnapshot.recover(snap_dir) do
      install_snapshot(recovered_meta, mac_state, leader_id, state)
    else
      err -> snapshot_failure(leader_id, state, err)
    end
  end

  defp install_snapshot(meta, mac_state, leader_id, state) do
    Metrics.inc(:snapshot_chunks_installed)
    %{index: snap_idx, term: snap_term} = meta
    old_mac_state = Map.get(state, :machine_state)
    {:ok, log, _} = Log.install_snapshot({snap_idx, snap_term}, nil, [], state.log)

    # Make the installed snapshot self-consistent: the log's term tracking
    # and snapshot state must reflect the snapshot, and the reply must
    # carry the snapshot term so the leader resumes from the right index.
    # The snapshot also becomes this server's future send source.
    log = %{
      log
      | last_term: snap_term,
        last_written_index_term: {snap_idx, snap_term},
        snapshot_state: mac_state
    }

    state =
      state
      |> Map.put(:log, log)
      |> Map.put(:machine_state, mac_state)
      |> Map.put(:snapshot_state, nil)
      |> Map.put(:snapshot_meta, nil)
      |> Map.put(:last_applied, max(Map.get(state, :last_applied, 0), snap_idx))
      |> Map.put(:commit_index, max(Map.get(state, :commit_index, 0), snap_idx))
      |> adopt_snapshot_cluster(meta)

    if is_map(state.cfg) && state.cfg.effective_machine_module do
      Machine.snapshot_installed(
        state.cfg.effective_machine_module,
        meta,
        mac_state,
        %{},
        old_mac_state
      )
    end

    {last_index, last_term} = Log.last_index_term(log)
    reply = build_install_snapshot_reply(state.current_term, state, last_index, last_term)
    {:follower, state, [{:send_rpc, leader_id, reply}]}
  end

  # Adopt the membership recorded in the snapshot when it is a well-formed
  # member map; the snapshot reflects the cluster at its index, which a
  # lagging follower has not yet replicated. Progress fields are reset to
  # defaults while membership roles (and uids) are preserved.
  defp adopt_snapshot_cluster(state, meta) do
    case Map.get(meta, :cluster) do
      cluster when is_map(cluster) and map_size(cluster) > 0 ->
        self_id =
          case state do
            %{cfg: %{id: id}} -> id
            _ -> nil
          end

        peers =
          Map.new(cluster, fn {id, peer} ->
            membership =
              case peer do
                %{voter_status: %{membership: m}} -> m
                _ -> :voter
              end

            uid =
              case peer do
                %{voter_status: %{uid: u}} -> u
                _ -> nil
              end

            {id,
             Cluster.new_peer() |> Map.put(:voter_status, %{membership: membership, uid: uid})}
          end)

        peers =
          if self_id != nil and not Map.has_key?(peers, self_id) do
            Map.put(peers, self_id, Cluster.new_peer())
          else
            peers
          end

        Map.put(state, :cluster, peers)

      _ ->
        state
    end
  end

  defp snapshot_failure(leader_id, state, reason) do
    Metrics.inc(:snapshot_chunks_failed)
    Logger.error("RaftEx.RpcHandler: snapshot transfer failed: #{inspect(reason)}")
    reply = build_install_snapshot_reply(state.current_term, state, 0, 0)

    # Discard only the partial transfer; a previously installed snapshot
    # must survive a failed replacement.
    case Map.get(state, :snapshot_state) do
      accept when is_tuple(accept) -> LogSnapshot.abort(accept)
      _ -> :ok
    end

    state = state |> Map.put(:snapshot_state, nil) |> Map.put(:snapshot_meta, nil)
    {:follower, state, [{:send_rpc, leader_id, reply}]}
  end

  defp snapshot_dir(%{log: %{data_dir: dir}}) when is_binary(dir) do
    Path.join(dir, "snapshot")
  end

  defp snapshot_dir(%{cfg: %{system_config: %{data_dir: dir}}}) when is_binary(dir), do: dir

  defp snapshot_dir(_state) do
    Path.join(System.tmp_dir!(), "raft_ex_snapshot")
  end

  @doc """
  Handle an InstallSnapshot reply from a follower.

  Returns `{updated_state, effects}`.
  """
  @spec handle_install_snapshot_reply(Types.InstallSnapshotResult.t(), map()) :: {map(), [term()]}
  def handle_install_snapshot_reply(
        %Types.InstallSnapshotResult{
          term: reply_term,
          last_index: last_index,
          last_term: _last_term
        },
        %{current_term: current_term} = state
      ) do
    cond do
      reply_term > current_term ->
        state = update_term(state, reply_term)
        {state, [{:become, :follower}]}

      reply_term == current_term ->
        peer_id = state.rpc_from

        if is_nil(peer_id) or not is_map_key(state.cluster, peer_id) do
          # Stale reply from an unknown or removed peer: ignore it instead
          # of re-adding the peer to the membership.
          {state, []}
        else
          now_ms = System.monotonic_time(:millisecond)

          cluster =
            Cluster.update_peer(
              peer_id,
              %{
                match_index: last_index,
                next_index: last_index + 1,
                status: :normal,
                last_ack: now_ms
              },
              state.cluster
            )

          {%{state | cluster: cluster}, []}
        end

      reply_term < current_term ->
        {state, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Heartbeat RPC Handling
  # ---------------------------------------------------------------------------

  @doc """
  Handle an incoming Heartbeat RPC from the leader.

  Returns `{updated_state, effects}`.
  """
  @spec handle_heartbeat_rpc(Types.HeartbeatRpc.t(), map()) :: {map(), [term()]}
  def handle_heartbeat_rpc(
        %Types.HeartbeatRpc{
          term: leader_term,
          leader_id: leader_id,
          query_index: query_index,
          commit_index: commit_index
        },
        %{current_term: current_term} = state
      ) do
    cond do
      leader_term < current_term ->
        reply = %Types.HeartbeatReply{
          query_index: 0,
          term: current_term
        }

        effects = [{:send_rpc, leader_id, reply}]
        {state, effects}

      leader_term > current_term ->
        old_commit = Map.get(state, :commit_index, 0)
        new_commit = max(old_commit, commit_index || 0)

        state =
          state
          |> adopt_term(leader_term)
          |> Map.put(:leader_id, leader_id)
          |> Map.put(:commit_index, new_commit)

        reply = %Types.HeartbeatReply{
          query_index: query_index,
          term: leader_term
        }

        apply_effects =
          if new_commit > old_commit, do: [{:apply_committed, new_commit}], else: []

        {state, [{:send_rpc, leader_id, reply}, {:reset_election_timeout} | apply_effects]}

      leader_term == current_term ->
        old_commit = Map.get(state, :commit_index, 0)
        new_commit = max(old_commit, commit_index || 0)

        state =
          state
          |> observe_term(leader_term)
          |> Map.put(:leader_id, leader_id)
          |> Map.put(:commit_index, new_commit)

        reply = %Types.HeartbeatReply{
          query_index: query_index,
          term: leader_term
        }

        apply_effects =
          if new_commit > old_commit, do: [{:apply_committed, new_commit}], else: []

        {state, [{:send_rpc, leader_id, reply}, {:reset_election_timeout} | apply_effects]}
    end
  end

  @doc """
  Handle a Heartbeat reply from a follower.

  Returns `{updated_state, effects}`.
  """
  @spec handle_heartbeat_reply(Types.HeartbeatReply.t(), map()) :: {map(), [term()]}
  def handle_heartbeat_reply(
        %Types.HeartbeatReply{
          term: reply_term,
          query_index: query_index
        },
        %{current_term: current_term, cluster: cluster0} = state
      ) do
    cond do
      reply_term > current_term ->
        state = update_term(state, reply_term)
        {state, [{:become, :follower}]}

      reply_term == current_term ->
        peer_id = state.rpc_from

        if is_nil(peer_id) or not is_map_key(cluster0, peer_id) do
          # Stale reply from an unknown or removed peer: ignore it instead
          # of re-adding the peer to the membership.
          {state, []}
        else
          now_ms = System.monotonic_time(:millisecond)

          cluster =
            Cluster.update_peer(
              peer_id,
              %{
                query_index: query_index,
                last_ack: now_ms
              },
              cluster0
            )

          {Map.put(state, :cluster, cluster), []}
        end

      reply_term < current_term ->
        {state, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Helper Functions
  # ---------------------------------------------------------------------------

  defp check_log_consistency(prev_log_index, prev_log_term, %{log: log} = state) do
    # No previous log to check
    if prev_log_index == 0 do
      {true, state}
    else
      # Check if we have the entry at prev_log_index
      case Log.fetch_term(prev_log_index, log) do
        {^prev_log_term, _} ->
          {true, state}

        {nil, _} ->
          # Entry doesn't exist
          {false, state}

        {_, _} ->
          # Term mismatch
          {false, state}
      end
    end
  end

  defp append_entries([], _log, state), do: {state.log, []}

  defp append_entries(entries, log, _state) do
    case first_missing_or_mismatched(entries, log) do
      nil ->
        # Every entry is already present with the same term.
        {log, []}

      {idx, _, _} ->
        # Everything from the first conflict onward is replaced atomically:
        # retain the prefix and append the surviving suffix in one
        # temp-write/rename step, so a crash cannot lose durable entries.
        entries_to_write = Enum.drop_while(entries, fn {i, _, _} -> i < idx end)

        case Log.replace_from(idx, entries_to_write, log) do
          {:ok, new_log} ->
            effects = [{:ra_log_event, {:written, entries_to_write}}]
            {new_log, effects}

          {:error, reason} ->
            Logger.error("Failed to write log entries: #{inspect(reason)}")
            {log, [{:append_failed, reason}]}
        end
    end
  end

  defp first_missing_or_mismatched(entries, log) do
    Enum.find(entries, fn {idx, term, _cmd} ->
      case Log.fetch_term(idx, log) do
        {^term, _} -> false
        _ -> true
      end
    end)
  end

  defp update_commit_index(leader_commit, %{log: log, commit_index: commit_index} = state) do
    if leader_commit > commit_index do
      {last_index, _} = Log.last_index_term(log)
      new_commit_index = min(leader_commit, last_index)

      state = %{state | commit_index: new_commit_index}
      effects = [{:apply_committed, new_commit_index}]

      {state, effects}
    else
      {state, []}
    end
  end

  defp maybe_advance_commit_index(%{cluster: cluster, cfg: %Config{id: id}, log: log} = state) do
    # Collect match indexes from voters
    {last_index, _} = Log.last_index_term(log)

    match_indexes =
      Cluster.voter_match_indexes(id, cluster, last_index)

    # Find the highest index replicated on a majority
    new_commit_index = Cluster.agreed_commit(match_indexes)

    # Both commit paths share one helper: only a current-term entry may
    # commit, and the index never moves backwards (9.7).
    case Server.advance_commit_index(state, new_commit_index) do
      {true, state} ->
        # The commit rides on the next AppendEntries/heartbeat instead of a
        # dedicated broadcast round (9.6); local application is immediate.
        effects = [
          {:apply_committed, new_commit_index},
          {:next_event, :info, :pipeline_rpcs}
        ]

        {state, effects}

      {false, state} ->
        {state, []}
    end
  end

  defp find_next_index(%{log: log} = state) do
    {last_index, _} = Log.last_index_term(log)
    next_index = last_index + 1
    {next_index, state}
  end

  # Advance `match_index` on a successful acknowledgement. A success reply
  # for `last_index` confirms the follower's log matches the leader's
  # through that index (log matching property), so match jumps there
  # directly instead of walking a contiguous prefix one range at a time.
  # Confirmed ranges leave the in-flight window and the ack set is pruned
  # to the remainder, keeping both bounded no matter how acks interleave
  # with resends and failure backtracks (9.3). Runs in amortized O(1) per
  # acknowledgement: each queued range is dropped at most once.
  defp advance_match(match_index, in_flight, acked, last_index) do
    list =
      if :queue.is_queue(in_flight) do
        :queue.to_list(in_flight)
      else
        in_flight
      end

    m = max(match_index, last_index)
    rest = Enum.reject(list, fn {_p, l} -> l <= m end)
    pruned = MapSet.filter(acked, &(&1 > m))
    {m, :queue.from_list(rest), pruned}
  end

  @doc """
  Adopt a strictly greater term: persist it, clear the vote, and clear
  leader/transfer state. Callers must only invoke this when `new_term` is
  greater than the current term; same-term traffic must use `observe_term/2`.
  """
  def adopt_term(state, new_term) do
    cfg = state.cfg
    uid = cfg.uid
    meta = Server.meta_name(cfg)

    _ =
      try do
        with :ok <- LogMeta.store_sync(meta, uid, :current_term, new_term),
             :ok <- LogMeta.store_sync(meta, uid, :voted_for, nil) do
          :ok
        else
          {:error, reason} ->
            require Logger

            Logger.error(
              "RaftEx.RpcHandler: failed to persist term #{new_term}: #{inspect(reason)}"
            )

            {:error, reason}
        end
      catch
        _, reason ->
          require Logger
          Logger.error("RaftEx.RpcHandler: term persistence raised: #{inspect(reason)}")
          {:error, reason}
      end

    new_state =
      state
      |> Map.put(:current_term, new_term)
      |> Map.put(:voted_for, nil)
      |> Map.put(:leader_id, nil)
      |> Map.put(:transfer_leadership, nil)

    case new_state do
      %{cfg: %{id: id}} -> Counters.set_gauge(id, :term, new_term)
      _ -> :ok
    end

    new_state
  end

  @doc """
  Observe traffic at the current term without touching durable term/vote
  state. Returns the state unchanged so a same-term heartbeat or snapshot
  can never clear `voted_for`.
  """
  def observe_term(state, _term), do: state

  defp update_term(state, new_term), do: adopt_term(state, new_term)

  defp build_append_entries_reply(
         term,
         success,
         state,
         last_index \\ nil,
         last_term \\ nil,
         next_index \\ nil
       ) do
    {li, lt} =
      if last_index && last_term do
        {last_index, last_term}
      else
        Log.last_index_term(state.log)
      end

    %Types.AppendEntriesReply{
      term: term,
      success: success,
      next_index: next_index || li + 1,
      last_index: li,
      last_term: lt
    }
  end

  defp build_install_snapshot_reply(term, _state, last_index, last_term) do
    {li, lt} =
      if last_index && last_term do
        {last_index, last_term}
      else
        {0, 0}
      end

    %Types.InstallSnapshotResult{
      term: term,
      last_index: li,
      last_term: lt
    }
  end
end
