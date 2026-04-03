defmodule RaftEx.Server.RpcHandler do
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

  alias RaftEx.Types
  alias RaftEx.Server.Config
  alias RaftEx.Server.Cluster
  alias RaftEx.Log

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
          leader_commit: leader_commit,
          entries: entries
        },
        %{cfg: %Config{id: id}, current_term: current_term, log: log0} = state
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
         %{log: log0, cfg: %Config{id: id}} = state
       ) do
    # Update leader info
    state = %{state | leader_id: leader_id}

    # Check log consistency
    {log_consistent, state} = check_log_consistency(prev_log_index, prev_log_term, state)

    if log_consistent do
      # Append new entries if any
      {log, append_effects} = append_entries(entries, log0, state)
      state = %{state | log: log}

      # Update commit index
      {state, commit_effects} = update_commit_index(leader_commit, state)

      # Build success reply
      {last_index, last_term} = Log.last_index_term(log)
      reply = build_append_entries_reply(state.current_term, true, state, last_index, last_term)

      effects = [{:send_rpc, leader_id, reply}] ++ append_effects ++ commit_effects

      {:follower, state, effects}
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
          last_term: last_term
        },
        %{cfg: %Config{id: id}, current_term: current_term, cluster: cluster0} = state
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

        cluster =
          cluster0
          |> Cluster.update_peer(peer_id, %{
            match_index: last_index,
            next_index: next_index,
            status: :normal
          })

        state = %{state | cluster: cluster}

        # Try to advance commit index
        {state, effects} = maybe_advance_commit_index(state)

        {state, effects}

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

        # Use next_index hint if provided, otherwise decrement
        new_next_index =
          if next_index && next_index > 0 do
            next_index
          else
            max(1, state.cluster[peer_id].next_index - 1)
          end

        cluster =
          Cluster.update_peer(state.cluster, peer_id, %{
            next_index: new_next_index,
            status: :normal
          })

        state = %{state | cluster: cluster}

        # Schedule retry
        effects = [{:send_append_entries, peer_id}]

        {state, effects}

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
        %{current_term: current_term, log: log0} = state
      ) do
    cond do
      # Leader's term is behind - reject
      leader_term < current_term ->
        reply = build_install_snapshot_reply(current_term, state)
        effects = [{:send_rpc, leader_id, reply}]
        {:follower, state, effects}

      # Leader's term is ahead or equal - process snapshot
      leader_term >= current_term ->
        state = update_term(state, leader_term)
        state = %{state | leader_id: leader_id}

        case chunk_state do
          :first ->
            handle_first_snapshot_chunk(meta, data, leader_id, state)

          :middle ->
            handle_middle_snapshot_chunk(data, state)

          :last ->
            handle_last_snapshot_chunk(data, leader_id, state)

          :full ->
            handle_full_snapshot(meta, data, leader_id, state)
        end
    end
  end

  defp handle_first_snapshot_chunk(meta, data, leader_id, %{cfg: %Config{id: id}} = state) do
    # Begin accepting snapshot
    case RaftEx.LogSnapshot.begin_accept(state.cfg.system_config.data_dir, meta) do
      {:ok, snapshot_state} ->
        # Write first chunk
        case RaftEx.LogSnapshot.accept_chunk(data, snapshot_state) do
          {:ok, new_snapshot_state} ->
            state = %{state | snapshot_state: new_snapshot_state}
            effects = []
            {:receive_snapshot, state, effects}

          {:error, reason} ->
            Logger.error("Failed to write snapshot chunk: #{inspect(reason)}")
            reply = build_install_snapshot_reply(state.current_term, state)
            effects = [{:send_rpc, leader_id, reply}]
            {:follower, state, effects}
        end

      {:error, reason} ->
        Logger.error("Failed to begin snapshot accept: #{inspect(reason)}")
        reply = build_install_snapshot_reply(state.current_term, state)
        effects = [{:send_rpc, leader_id, reply}]
        {:follower, state, effects}
    end
  end

  defp handle_middle_snapshot_chunk(data, %{snapshot_state: snapshot_state} = state) do
    case RaftEx.LogSnapshot.accept_chunk(data, snapshot_state) do
      {:ok, new_snapshot_state} ->
        state = %{state | snapshot_state: new_snapshot_state}
        {:receive_snapshot, state, []}

      {:error, reason} ->
        Logger.error("Failed to write snapshot chunk: #{inspect(reason)}")
        {:follower, state, []}
    end
  end

  defp handle_last_snapshot_chunk(data, leader_id, %{snapshot_state: snapshot_state} = state) do
    case RaftEx.LogSnapshot.complete_accept(data, snapshot_state) do
      {:ok, _bytes} ->
        # Install the snapshot
        meta = RaftEx.LogSnapshot.read_meta(state.cfg.system_config.data_dir)

        case meta do
          {:ok, %{index: snap_idx, term: snap_term}} ->
            {log, _} = Log.install_snapshot({snap_idx, snap_term}, nil, [], state.log)
            state = %{state | log: log, snapshot_state: nil}

            {last_index, last_term} = Log.last_index_term(log)
            reply = build_install_snapshot_reply(state.current_term, state, last_index, last_term)
            effects = [{:send_rpc, leader_id, reply}]
            {:follower, state, effects}

          _ ->
            {:follower, state, []}
        end

      {:error, reason} ->
        Logger.error("Failed to complete snapshot: #{inspect(reason)}")
        {:follower, state, []}
    end
  end

  defp handle_full_snapshot(meta, data, leader_id, state) do
    # Write complete snapshot in one go
    case RaftEx.LogSnapshot.write(state.cfg.system_config.data_dir, meta, data, true) do
      {:ok, _bytes} ->
        %{index: snap_idx, term: snap_term} = meta
        {log, _} = Log.install_snapshot({snap_idx, snap_term}, nil, [], state.log)
        state = %{state | log: log}

        {last_index, last_term} = Log.last_index_term(log)
        reply = build_install_snapshot_reply(state.current_term, state, last_index, last_term)
        effects = [{:send_rpc, leader_id, reply}]
        {:follower, state, effects}

      {:error, reason} ->
        Logger.error("Failed to write full snapshot: #{inspect(reason)}")
        reply = build_install_snapshot_reply(state.current_term, state)
        effects = [{:send_rpc, leader_id, reply}]
        {:follower, state, effects}
    end
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
          last_term: last_term
        },
        %{current_term: current_term} = state
      ) do
    cond do
      reply_term > current_term ->
        state = update_term(state, reply_term)
        {state, [{:become, :follower}]}

      reply_term == current_term ->
        peer_id = state.rpc_from

        cluster =
          Cluster.update_peer(state.cluster, peer_id, %{
            match_index: last_index,
            next_index: last_index + 1,
            status: :normal
          })

        state = %{state | cluster: cluster}
        {state, []}

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
          query_index: query_index
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

      leader_term >= current_term ->
        state = update_term(state, leader_term)
        state = %{state | leader_id: leader_id}

        reply = %Types.HeartbeatReply{
          query_index: query_index,
          term: leader_term
        }

        effects = [{:send_rpc, leader_id, reply}]
        {state, effects}
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

        cluster =
          Cluster.update_peer(cluster0, peer_id, %{
            query_index: query_index
          })

        state = %{state | cluster: cluster}
        {state, []}

      reply_term < current_term ->
        {state, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Helper Functions
  # ---------------------------------------------------------------------------

  defp check_log_consistency(prev_log_index, prev_log_term, %{log: log} = state) do
    cond do
      # No previous log to check
      prev_log_index == 0 ->
        {true, state}

      # Check if we have the entry at prev_log_index
      true ->
        case Log.fetch_term(prev_log_index, log) do
          {^prev_log_term, _} ->
            {true, state}

          {_, _} ->
            # Term mismatch
            {false, state}

          {nil, _} ->
            # Entry doesn't exist
            {false, state}
        end
    end
  end

  defp append_entries([], log, state), do: {log, []}

  defp append_entries(entries, log0, state) do
    # Filter out entries we already have
    {new_entries, state} =
      Enum.reduce(entries, {[], state}, fn {idx, term, cmd} = entry, {acc, st} ->
        case Log.fetch_term(idx, st.log) do
          {^term, _} ->
            # Already have this entry with same term - skip
            {acc, st}

          _ ->
            # Need to append this entry
            # First, delete any conflicting entries after this index
            log = truncate_log_from(st.log, idx)
            st = %{st | log: log}

            # Append the entry
            new_log = Log.append(entry, st.log)
            st = %{st | log: new_log}

            {[entry | acc], st}
        end
      end)

    # Write entries to log
    if new_entries != [] do
      entries_to_write = Enum.reverse(new_entries)

      case Log.write(entries_to_write, state.log) do
        {:ok, new_log} ->
          state = %{state | log: new_log}
          effects = [{:ra_log_event, {:written, entries_to_write}}]
          {new_log, effects}

        {:error, reason} ->
          Logger.error("Failed to write log entries: #{inspect(reason)}")
          {state.log, []}
      end
    else
      {state.log, []}
    end
  end

  defp truncate_log_from(log, from_index) do
    # In a full implementation, this would truncate the log from from_index onwards
    # For now, we return the log as-is
    log
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

    if new_commit_index > state.commit_index do
      state = %{state | commit_index: new_commit_index}
      effects = [{:apply_committed, new_commit_index}]
      {state, effects}
    else
      {state, []}
    end
  end

  defp find_next_index(%{log: log} = state) do
    {last_index, _} = Log.last_index_term(log)
    next_index = last_index + 1
    {next_index, state}
  end

  defp update_term(state, new_term) do
    %{state | current_term: new_term, voted_for: nil}
  end

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

  defp build_install_snapshot_reply(term, state, last_index \\ nil, last_term \\ nil) do
    {li, lt} =
      if last_index && last_term do
        {last_index, last_term}
      else
        Log.last_index_term(state.log)
      end

    %Types.InstallSnapshotResult{
      term: term,
      last_index: li,
      last_term: lt
    }
  end
end
