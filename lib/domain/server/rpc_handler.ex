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
  alias RaftEx.Infra.Observability.Metrics
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

      # Update commit index
      {state, commit_effects} = update_commit_index(leader_commit, state)

      # Build success reply
      {last_index, last_term} = Log.last_index_term(log)
      reply = build_append_entries_reply(state.current_term, true, state, last_index, last_term)

      effects =
        [{:send_rpc, leader_id, reply}, {:reset_election_timeout}] ++
          append_effects ++ commit_effects

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

        Metrics.inc(:append_entries_acked)

        lag =
          case state do
            %{log: log} ->
              {li, _} = Log.last_index_term(log)
              max(li - last_index, 0)

            _ ->
              0
          end

        Metrics.set({:replication_lag_ms, peer_id}, lag)

        cluster =
          Cluster.update_peer(
            peer_id,
            %{
              match_index: last_index,
              next_index: next_index,
              status: :normal
            },
            cluster0
          )

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
          Cluster.update_peer(
            peer_id,
            %{
              next_index: new_next_index,
              status: :normal
            },
            state.cluster
          )

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
        %{current_term: current_term, log: _log0} = state
      ) do
    cond do
      # Leader's term is behind - reject
      leader_term < current_term ->
        reply = build_install_snapshot_reply(current_term, state, 0, 0)
        state = state |> Map.put(:snapshot_state, nil) |> Map.put(:snapshot_meta, nil)
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

    data =
      if byte_size(data) >= 4,
        do: binary_part(data, 4, byte_size(data) - 4),
        else: data

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

    state =
      state
      |> Map.put(:log, log)
      |> Map.put(:machine_state, mac_state)
      |> Map.put(:snapshot_state, nil)
      |> Map.put(:snapshot_meta, nil)
      |> Map.put(:last_applied, max(Map.get(state, :last_applied, 0), snap_idx))
      |> Map.put(:commit_index, max(Map.get(state, :commit_index, 0), snap_idx))

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

  defp snapshot_failure(leader_id, state, reason) do
    Metrics.inc(:snapshot_chunks_failed)
    Logger.error("RaftEx.RpcHandler: snapshot transfer failed: #{inspect(reason)}")
    reply = build_install_snapshot_reply(state.current_term, state, 0, 0)

    case snapshot_dir(state) do
      dir -> File.rm(Path.join(dir, "snapshot.dat"))
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

        cluster =
          Cluster.update_peer(
            peer_id,
            %{
              match_index: last_index,
              next_index: last_index + 1,
              status: :normal
            },
            state.cluster
          )

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

      leader_term >= current_term ->
        old_commit = Map.get(state, :commit_index, 0)
        new_commit = max(old_commit, commit_index || 0)

        state =
          state
          |> update_term(leader_term)
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

        cluster =
          Cluster.update_peer(
            peer_id,
            %{
              query_index: query_index
            },
            cluster0
          )

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

  defp append_entries(entries, _log0, state) do
    # Filter out entries we already have
    {new_entries, state} =
      Enum.reduce(entries, {[], state}, fn {idx, term, _cmd} = entry, {acc, st} ->
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
          _state = %{state | log: new_log}
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
    Log.truncate(from_index, log)
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

      effects = [
        {:apply_committed, new_commit_index},
        {:broadcast_commit, new_commit_index}
      ]

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
    cfg = state.cfg
    uid = cfg.uid
    meta = Server.meta_name(cfg)

    try do
      LogMeta.store(meta, uid, :current_term, new_term)
      LogMeta.store_sync(meta, uid, :voted_for, nil)
    catch
      _, _ -> :ok
    end

    state
    |> Map.put(:current_term, new_term)
    |> Map.put(:voted_for, nil)
    |> Map.put(:leader_id, nil)
    |> Map.put(:transfer_leadership, nil)
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
