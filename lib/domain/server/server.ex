defmodule RaftEx.Domain.Server do
  @moduledoc """
  Core Raft server logic: leader election, log replication, and state machine
  application.

  This module is a pure-functional layer — it receives messages, computes the
  next state and a list of *effects*, and returns them to the caller
  (`RaftEx.App.ServerRuntime`), which owns the OTP process and actually executes the
  effects.

  ## Server state map keys

  | Key                          | Description                                      |
  |------------------------------|--------------------------------------------------|
  | `:cfg`                       | `RaftEx.Domain.Server.Config` struct (immutable mostly) |
  | `:leader_id`                 | Current known leader, or `nil`                   |
  | `:current_term`              | Raft term counter                                |
  | `:cluster`                   | `%{server_id => peer_state}`                     |
  | `:cluster_change_permitted`  | Whether a cluster change may be appended now     |
  | `:cluster_index_term`        | `{index, term}` of the last cluster-change entry |
  | `:voted_for`                 | Server voted for in current term, or `nil`       |
  | `:membership`                | This server's own membership role                |
  | `:commit_index`              | Highest log index known to be committed          |
  | `:last_applied`              | Highest index applied to the state machine       |
  | `:persisted_last_applied`    | Value of `:last_applied` written to durable meta |
  | `:log`                       | `RaftEx.Infra.Persistence.Log` state                               |
  | `:machine_state`             | User state machine state                         |
  | `:aux_state`                 | Auxiliary state machine state                    |
  | `:query_index`               | Monotonic index for consistent query tracking    |
  | `:queries_waiting_heartbeats`| Queue of `{query_index, query_ref}` tuples       |
  | `:pending_consistent_queries`| Queries queued awaiting cluster-change perm      |
  """

  require Logger

  alias RaftEx.Domain.{Conn, Invariant, Machine, Pipeline, Types}
  alias RaftEx.Domain.Server.{Cluster, Config, Effects, Election, RpcHandler}
  alias RaftEx.Domain.Utils
  alias RaftEx.Infra.Observability.{Counters, Metrics}
  alias RaftEx.Infra.Persistence.{Log, LogMeta}

  # ---------------------------------------------------------------------------
  # Initialisation
  # ---------------------------------------------------------------------------

  @doc "Build the initial server state from a configuration map."
  @spec init(map()) :: map()
  def init(
        %{
          id: id,
          uid: uid,
          cluster_name: _cn,
          initial_members: initial_nodes,
          log_init_args: log_init_args,
          machine: machine_conf
        } = config
      ) do
    system_config = Map.get(config, :system_config, RaftEx.App.System.default_config())
    log_id = Map.get(config, :friendly_name, inspect(id))

    raw_command_plugs =
      Map.get(config, :command_plugs, Map.get(system_config, :command_plugs, []))

    raw_rpc_plugs = Map.get(config, :rpc_plugs, Map.get(system_config, :rpc_plugs, []))
    command_plugs = Pipeline.init_plugs(raw_command_plugs)
    rpc_plugs = Pipeline.init_plugs(raw_rpc_plugs)
    machine = normalise_machine(machine_conf)
    snap_module = Machine.snapshot_module(machine)
    counter = Map.get(config, :counter)

    log =
      Log.init(
        Map.merge(log_init_args, %{
          snapshot_module: snap_module,
          machine: machine,
          system_config: system_config,
          uid: uid,
          counter: counter,
          log_id: log_id,
          initial_access_pattern: :sequential,
          segment_storage:
            Map.get(config, :segment_storage, Map.get(system_config, :segment_storage, false)),
          log_window_size:
            Map.get(config, :log_window_size, Map.get(system_config, :log_window_size, 8192))
        })
      )

    meta_name = meta_name(system_config)
    current_term = LogMeta.fetch(meta_name, uid, :current_term, 0)
    last_applied = LogMeta.fetch(meta_name, uid, :last_applied, 0)
    voted_for = LogMeta.fetch(meta_name, uid, :voted_for, nil)
    latest_mac_ver = Machine.version(machine)
    initial_mac_ver = min(latest_mac_ver, Map.get(config, :initial_machine_version, 0))

    {cluster, eff_mac_ver, mac_state, {snap_idx, _} = snap_idx_term} =
      init_from_snapshot(log, machine, id, initial_nodes, initial_mac_ver)

    mac_mod = Machine.which_module(machine, eff_mac_ver)
    commit_index = max(last_applied, snap_idx)

    cfg = %Config{
      id: id,
      uid: uid,
      log_id: log_id,
      metrics_key: Map.get(config, :metrics_key, Utils.ra_server_id_to_local_name(id)),
      machine: machine,
      machine_version: latest_mac_ver,
      machine_versions: [{snap_idx, eff_mac_ver}],
      effective_machine_version: eff_mac_ver,
      effective_machine_module: mac_mod,
      effective_handle_aux_fun: Machine.which_aux_fun(mac_mod),
      max_pipeline_count: Map.get(config, :max_pipeline_count, 4096),
      max_append_entries_rpc_batch_size:
        Map.get(
          config,
          :max_append_entries_rpc_batch_size,
          Map.get(
            system_config,
            :max_append_entries_rpc_batch_size,
            Map.get(system_config, :default_max_append_entries_rpc_batch_size, 128)
          )
        ),
      counter: counter,
      system_config: system_config,
      command_plugs: command_plugs,
      rpc_plugs: rpc_plugs,
      compaction_entry_threshold:
        Map.get(
          config,
          :compaction_entry_threshold,
          Map.get(system_config, :compaction_entry_threshold, 8192)
        ),
      compaction_wal_bytes:
        Map.get(
          config,
          :compaction_wal_bytes,
          Map.get(system_config, :compaction_wal_bytes, 134_217_728)
        ),
      compaction_interval_ms:
        Map.get(
          config,
          :compaction_interval_ms,
          Map.get(system_config, :compaction_interval_ms, 120_000)
        )
    }

    now_ms = System.monotonic_time(:millisecond)

    %{
      cfg: cfg,
      leader_id: nil,
      current_term: current_term,
      cluster: cluster,
      cluster_change_permitted: false,
      cluster_index_term: snap_idx_term,
      voted_for: voted_for,
      membership: :voter,
      commit_index: commit_index,
      last_applied: snap_idx,
      persisted_last_applied: last_applied,
      commit_latency: 0,
      log: log,
      machine_state: mac_state,
      aux_state: Machine.init_aux(mac_mod, elem(id, 0)),
      query_index: 0,
      queries_waiting_heartbeats: :queue.new(),
      pending_consistent_queries: [],
      previous_cluster: nil,
      rpc_from: nil,
      initial_members: initial_nodes,
      last_auto_compact_ms: now_ms,
      entries_since_compact: 0
    }
  end

  # ---------------------------------------------------------------------------
  # Recovery
  # ---------------------------------------------------------------------------

  @doc "Replay committed log entries onto the state machine during startup."
  @spec recover(map()) :: map()
  def recover(
        %{
          cfg: %Config{log_id: log_id, machine_version: mac_ver},
          commit_index: commit_index,
          last_applied: last_applied,
          log: log
        } = state0
      ) do
    snap_state = Log.snapshot_state(log)

    {last_applied1, %{cfg: cfg} = state1} =
      maybe_recover_from_recovery_checkpoint(last_applied, commit_index, snap_state, state0)

    Logger.debug(
      "#{log_id}: recovering sm v#{cfg.effective_machine_version}:#{mac_ver} " <>
        "from #{last_applied1} to #{commit_index}"
    )

    {%{log: log1} = state2, _} =
      apply_committed_entries(commit_index, state1, [])

    # Scan entries above commit_index for cluster-change entries already in log.
    from_scan = commit_index + 1
    {to_scan, _} = Log.last_index_term(log1)

    {{last_scanned, state3}, log2} =
      Log.fold(from_scan, to_scan, &cluster_scan_fun/2, {commit_index, state2}, log1)

    state =
      if last_scanned < to_scan do
        {:ok, log3} = Log.set_last_index(last_scanned, log2)
        %{state3 | log: log3}
      else
        state3
      end

    new_state = Map.put(state, :commit_latency, 0)

    snap_idx =
      case Log.snapshot_index_term(new_state.log) do
        {idx, _} when is_integer(idx) -> idx
        _ -> 0
      end

    {lli, _} = Log.last_index_term(new_state.log)
    {lwi, _} = Log.last_written(new_state.log)

    Invariant.check_recovery!(%{
      snapshot_index: snap_idx,
      wal_persisted_index: lwi,
      log_last_index: lli,
      commit_index: new_state.commit_index,
      last_applied: new_state.last_applied
    })

    new_state
  end

  # ---------------------------------------------------------------------------
  # Leader message handlers
  # ---------------------------------------------------------------------------

  @doc false
  @spec handle_leader(term(), map()) :: {atom(), map(), list()}
  def handle_leader({:commands, cmds}, %{cfg: cfg} = state0) when is_list(cmds) do
    Enum.each(cmds, fn _ -> incr_counter(cfg, :commands, 1) end)

    # Normalize casts to the 4-tuple shape, then run each command through
    # the command plugs. Halted commands get their own error reply and
    # never reach the log.
    {ready, halted_effects} =
      Enum.reduce(cmds, {[], []}, fn cmd, {ok_acc, eff_acc} ->
        cmd =
          case cmd do
            {:"$usr", data, mode} -> {:"$usr", %{}, data, mode}
            _ -> cmd
          end

        conn = %Conn{type: :command, request: %{command: cmd}}
        conn = Pipeline.run(conn, cfg.command_plugs)

        if conn.halted do
          resp = conn.resp || {:error, :halted}

          eff =
            case cmd do
              {_, %{from: from}, _, _} when not is_nil(from) ->
                [{:reply, from, {:wrap_reply, resp}}]

              _ ->
                []
            end

          {ok_acc, eff ++ eff_acc}
        else
          {[Map.get(conn.request, :command, cmd) | ok_acc], eff_acc}
        end
      end)

    ready = Enum.reverse(ready)

    case ready do
      [] ->
        {:leader, state0, halted_effects}

      cmds when cmds != [] ->
        if Enum.all?(cmds, &match?({:"$usr", _, _, _}, &1)) do
          # Homogeneous user-command batch: one WAL write for all entries.
          {state, effects, write_result} = append_command_batch(cmds, state0)

          case write_result do
            :ok ->
              {:leader, state, halted_effects ++ effects}

            {:error, :wal_down} ->
              condition = %{predicate_fun: &wal_down_condition/2, transition_to: :leader}

              {:await_condition, Map.put(state, :condition, condition), halted_effects ++ effects}

            {:error, _} ->
              {:leader, state, halted_effects ++ effects}
          end
        else
          # Mixed batch (cluster changes): fall back to sequential handling
          # so single-change validation and ordering stay exactly as today.
          {state, effects} =
            Enum.reduce(cmds, {state0, halted_effects}, fn cmd, {st, effs} ->
              case handle_leader({:command, cmd}, st) do
                {:leader, st1, effs1} ->
                  {st1, effs ++ effs1}

                {:await_condition, st1, effs1} ->
                  {Map.put(st1, :__batch_await__, true), effs ++ effs1}

                {_, st1, effs1} ->
                  {st1, effs ++ effs1}
              end
            end)

          if Map.get(state, :__batch_await__, false) do
            state = Map.delete(state, :__batch_await__)
            condition = %{predicate_fun: &wal_down_condition/2, transition_to: :leader}
            {:await_condition, Map.put(state, :condition, condition), effects}
          else
            {:leader, state, effects}
          end
        end
    end
  end

  def handle_leader({:command, cmd}, %{cfg: cfg} = state0) do
    incr_counter(cfg, :commands, 1)

    # Casts carry no caller to attribute; normalize them to the same
    # 4-tuple shape calls use so they append and apply identically
    # (with an empty context instead of a caller reference).
    cmd =
      case cmd do
        {:"$usr", data, mode} -> {:"$usr", %{}, data, mode}
        _ -> cmd
      end

    conn = %Conn{type: :command, request: %{command: cmd}}
    conn = Pipeline.run(conn, cfg.command_plugs)

    if conn.halted do
      resp = conn.resp || {:error, :halted}

      effects =
        case cmd do
          {_, %{from: from}, _, _} when not is_nil(from) ->
            [{:reply, from, {:wrap_reply, resp}}]

          _ ->
            []
        end

      {:leader, state0, effects}
    else
      cmd = Map.get(conn.request, :command, cmd)

      case append_log_as_leader(cmd, state0, []) do
        {:ok, idx, term, state1, effects0} ->
          force = match?({:noop, _, _}, cmd)
          {state, _, effects1} = make_pipelined_rpc_effects(state1, effects0, force)
          {state2, effects2} = evaluate_quorum(state, effects1)
          effects = Effects.after_log_append_reply(cmd, idx, term, effects2)
          {:leader, state2, effects}

        {:not_appended, :wal_down, state, effects0} ->
          condition = %{predicate_fun: &wal_down_condition/2, transition_to: :leader}
          effects = Effects.append_error_reply(cmd, :wal_down, effects0)
          {:await_condition, Map.put(state, :condition, condition), effects}

        {:not_appended, reason, state, effects0} ->
          Logger.warning("#{state.cfg.log_id} command NOT appended: #{reason}")
          {:leader, state, Effects.append_error_reply(cmd, reason, effects0)}
      end
    end
  end

  def handle_leader({:ra_log_event, {:written, _, _} = evt}, %{log: log0} = state0) do
    {log, effects0} = Log.handle_event(evt, log0)
    {state1, effects1} = evaluate_quorum(%{state0 | log: log}, effects0)
    {state2, effects2} = process_pending_consistent_queries(state1, effects1)
    {state, effects} = process_pending_read_queries(state2, effects2)
    {:leader, state, [{:next_event, :info, :pipeline_rpcs} | effects]}
  end

  def handle_leader({:ra_log_event, :major_compaction}, state) do
    {state, effects} = maybe_checkpoint(state)
    {:leader, state, effects}
  end

  def handle_leader({:ra_log_event, evt}, %{log: log0} = state) do
    {log, effects} = Log.handle_event(evt, log0)
    {:leader, %{state | log: log}, effects}
  end

  def handle_leader(
        %Types.AppendEntriesReply{} = reply,
        %{rpc_from: peer_id, cluster: cluster} = state
      )
      when is_map_key(cluster, peer_id) do
    {state2, effects} = RpcHandler.handle_append_entries_reply(reply, state)

    if Enum.member?(effects, {:become, :follower}) do
      {:follower, state2, List.delete(effects, {:become, :follower})}
    else
      {:leader, state2, effects}
    end
  end

  def handle_leader(%Types.AppendEntriesReply{} = reply, state) do
    if reply.term > state.current_term do
      {new_state, effects} = adopt_term(reply.term, state, nil)
      {:follower, new_state, effects}
    else
      {:leader, state, []}
    end
  end

  def handle_leader(
        %Types.HeartbeatReply{} = reply,
        %{rpc_from: peer_id, cluster: cluster} = state
      )
      when is_map_key(cluster, peer_id) do
    if reply.term > state.current_term do
      {new_state, effects} = adopt_term(reply.term, state, nil)
      {:follower, new_state, effects}
    else
      now_ms = System.monotonic_time(:millisecond)

      cluster =
        Cluster.update_peer(
          peer_id,
          %{query_index: reply.query_index, last_ack: now_ms},
          cluster
        )

      state = %{state | cluster: cluster}
      {state, effects} = evaluate_consistent_heartbeats(state)
      {:leader, state, effects}
    end
  end

  def handle_leader(%Types.HeartbeatReply{} = reply, state) do
    if reply.term > state.current_term do
      {new_state, effects} = adopt_term(reply.term, state, nil)
      {:follower, new_state, effects}
    else
      {:leader, state, []}
    end
  end

  def handle_leader(%Types.RequestVoteRpc{} = rpc, state) do
    if rpc.term > state.current_term do
      {adopted, effects} = adopt_term(rpc.term, state, nil)
      {:follower, new_state, effects2} = handle_follower(rpc, adopted)
      {:follower, new_state, effects ++ effects2}
    else
      reply = %Types.RequestVoteResult{term: state.current_term, vote_granted: false}
      {:leader, state, [{:send_rpc, rpc.candidate_id, reply}]}
    end
  end

  def handle_leader(%Types.PreVoteRpc{} = rpc, state) do
    if rpc.term > state.current_term do
      {adopted, effects} = adopt_term(rpc.term, state, nil)
      {:follower, new_state, effects2} = handle_follower(rpc, adopted)
      {:follower, new_state, effects ++ effects2}
    else
      reply = %Types.PreVoteResult{
        term: state.current_term,
        token: Map.get(rpc, :token),
        vote_granted: false
      }

      {:leader, state, [{:send_rpc, rpc.candidate_id, reply}]}
    end
  end

  def handle_leader(%Types.AppendEntriesRpc{} = rpc, state) do
    if rpc.term > state.current_term do
      handle_follower(rpc, state)
    else
      reply = %Types.AppendEntriesReply{
        term: state.current_term,
        success: false,
        next_index: 0,
        last_index: 0,
        last_term: 0
      }

      {:leader, state, [{:send_rpc, rpc.leader_id, reply}]}
    end
  end

  def handle_leader(%Types.HeartbeatRpc{} = rpc, state) do
    if rpc.term > state.current_term do
      handle_follower(rpc, state)
    else
      {:leader, state, []}
    end
  end

  def handle_leader(%Types.InstallSnapshotRpc{} = rpc, state) do
    if rpc.term > state.current_term do
      handle_follower(rpc, state)
    else
      {:leader, state, []}
    end
  end

  def handle_leader(%Types.TimeoutNowRpc{} = rpc, state) do
    if rpc.term > state.current_term do
      {adopted, effects} = adopt_term(rpc.term, state, nil)
      {:follower, new_state, effects2} = handle_follower(rpc, adopted)
      {:follower, new_state, effects ++ effects2}
    else
      {:leader, state, []}
    end
  end

  def handle_leader(
        %Types.InstallSnapshotResult{} = reply,
        %{rpc_from: peer_id, cluster: cluster} = state
      )
      when is_map_key(cluster, peer_id) do
    if reply.term > state.current_term do
      {new_state, effects} = adopt_term(reply.term, state, nil)
      {:follower, new_state, effects}
    else
      now_ms = System.monotonic_time(:millisecond)

      cluster =
        Cluster.update_peer(
          peer_id,
          %{
            match_index: reply.last_index,
            next_index: reply.last_index + 1,
            status: :normal,
            last_ack: now_ms
          },
          state.cluster
        )

      state = %{state | cluster: cluster}
      {:leader, state, [{:clear_snapshot_send, peer_id}, {:next_event, :info, :pipeline_rpcs}]}
    end
  end

  def handle_leader({:local_query, query_fun}, state) do
    local_query_result(query_fun, state, :leader)
  end

  def handle_leader({:local_query, query_fun, _opts}, state) do
    local_query_result(query_fun, state, :leader)
  end

  def handle_leader({:consistent_query, query_fun}, state) do
    consistent_query_admit(query_fun, state)
  end

  def handle_leader({:consistent_aux, aux_cmd}, state) do
    consistent_aux_admit(aux_cmd, state)
  end

  def handle_leader({:aux_command, cmd}, state) do
    {_, state, effects} = handle_aux(:leader, :call, cmd, state)
    {:leader, state, effects}
  end

  def handle_leader({:aux_command, cmd, _opts}, state) do
    {_, state, effects} = handle_aux(:leader, :call, cmd, state)
    {:leader, state, effects}
  end

  def handle_leader({:transfer_leadership, target}, state) do
    transfer_leadership(target, state)
  end

  def handle_leader(_msg, state), do: {:leader, state, []}

  @doc "Adopt a higher term and optionally record a vote, returning durable store_meta effects."
  @spec adopt_term(RaftEx.Domain.Types.term_num(), map(), term()) :: {map(), [term()]}
  def adopt_term(term, %{current_term: cur} = state, voted_for \\ nil) do
    if term > cur do
      effects = [{:store_meta, :current_term, term}, {:store_meta, :voted_for, voted_for}]
      state = %{state | current_term: term, voted_for: voted_for, leader_id: nil}

      case state do
        %{cfg: %{id: id}} -> Counters.set_gauge(id, :term, term)
        _ -> :ok
      end

      {state, effects}
    else
      {state, []}
    end
  end

  # ---------------------------------------------------------------------------
  # Follower message handlers
  # ---------------------------------------------------------------------------

  @doc false
  @spec handle_follower(term(), map()) :: {atom(), map(), list()}
  def handle_follower(%Types.AppendEntriesRpc{} = rpc, state0) do
    case run_rpc_pipeline(rpc, state0) do
      {:halted, conn} ->
        rpc = Map.get(conn.request, :rpc, rpc)

        reply =
          conn.resp ||
            %Types.AppendEntriesReply{
              term: state0.current_term,
              success: false,
              next_index: 0,
              last_index: 0,
              last_term: 0
            }

        {:follower, state0, [{:send_rpc, rpc.leader_id, reply}]}

      {:cont, rpc} ->
        RpcHandler.handle_append_entries_rpc(rpc, state0)
    end
  end

  def handle_follower(%Types.PreVoteRpc{} = rpc, state) do
    case run_rpc_pipeline(rpc, state) do
      {:halted, _} ->
        reply = %Types.PreVoteResult{
          term: state.current_term,
          token: rpc.token,
          vote_granted: false
        }

        {:follower, state, [{:send_rpc, rpc.candidate_id, reply}]}

      {:cont, rpc} ->
        {granted, effects0} = Election.handle_pre_vote_request(rpc, state)

        reply = %Types.PreVoteResult{
          term: state.current_term,
          token: rpc.token,
          vote_granted: granted
        }

        {:follower, state, effects0 ++ [{:send_rpc, rpc.candidate_id, reply}]}
    end
  end

  def handle_follower(%Types.RequestVoteRpc{} = rpc, state) do
    case run_rpc_pipeline(rpc, state) do
      {:halted, _} ->
        reply = %Types.RequestVoteResult{term: state.current_term, vote_granted: false}
        {:follower, state, [{:send_rpc, rpc.candidate_id, reply}]}

      {:cont, rpc} ->
        {granted, new_state, effects} = Election.handle_vote_request(rpc, state)

        reply = %Types.RequestVoteResult{
          term: new_state.current_term,
          vote_granted: granted
        }

        {:follower, new_state, effects ++ [{:send_rpc, rpc.candidate_id, reply}]}
    end
  end

  def handle_follower(%Types.InstallSnapshotRpc{} = rpc, state) do
    case run_rpc_pipeline(rpc, state) do
      {:halted, _} ->
        reply = %Types.InstallSnapshotResult{
          term: state.current_term,
          last_index: 0,
          last_term: 0
        }

        {:follower, state, [{:send_rpc, rpc.leader_id, reply}]}

      {:cont, rpc} ->
        RpcHandler.handle_install_snapshot_rpc(rpc, state)
    end
  end

  def handle_follower(%Types.TimeoutNowRpc{} = rpc, state) do
    case run_rpc_pipeline(rpc, state) do
      {:halted, _} ->
        {:follower, state, []}

      {:cont, rpc} ->
        do_handle_timeout_now(rpc, state)
    end
  end

  def handle_follower(%Types.HeartbeatRpc{} = rpc, state) do
    case run_rpc_pipeline(rpc, state) do
      {:halted, _} ->
        reply = %Types.HeartbeatReply{query_index: 0, term: state.current_term}
        {:follower, state, [{:send_rpc, rpc.leader_id, reply}]}

      {:cont, rpc} ->
        {state, effects} = RpcHandler.handle_heartbeat_rpc(rpc, state)
        {:follower, state, effects}
    end
  end

  def handle_follower({:ra_log_event, :major_compaction}, state) do
    {state, effects} = maybe_checkpoint(state)
    {:follower, state, effects}
  end

  def handle_follower(:election_timeout, state) do
    if voter?(state) do
      call_for_election(:pre_vote, state)
    else
      {:follower, state, []}
    end
  end

  def handle_follower({:local_query, query_fun}, state) do
    local_query_result(query_fun, state, :follower)
  end

  def handle_follower({:local_query, query_fun, _opts}, state) do
    local_query_result(query_fun, state, :follower)
  end

  def handle_follower({:aux_command, cmd}, state) do
    {_, state, effects} = handle_aux(:follower, :call, cmd, state)
    {:follower, state, effects}
  end

  def handle_follower({:aux_command, cmd, _opts}, state) do
    {_, state, effects} = handle_aux(:follower, :call, cmd, state)
    {:follower, state, effects}
  end

  def handle_follower(_msg, state), do: {:follower, state, []}

  # Runs the configured RPC plug pipeline over an incoming RPC. Returns
  # `{:halted, conn}` when a plug halted the request and `{:cont, rpc}`
  # (possibly transformed by plugs) otherwise. With the default empty
  # pipeline the request passes through unchanged.
  @spec run_rpc_pipeline(map(), map()) :: {:halted, Conn.t()} | {:cont, map()}
  defp run_rpc_pipeline(rpc, %{cfg: cfg}) do
    conn = %Conn{type: :rpc, request: %{rpc: rpc}}
    conn = Pipeline.run(conn, cfg.rpc_plugs)

    if conn.halted do
      {:halted, conn}
    else
      {:cont, Map.get(conn.request, :rpc, rpc)}
    end
  end

  defp do_handle_timeout_now(rpc, state) do
    cond do
      rpc.term != state.current_term ->
        {:follower, state, []}

      state.leader_id == nil or rpc.leader_id != state.leader_id ->
        {:follower, state, []}

      true ->
        Metrics.inc(:leadership_transfers_completed)

        # Persist the observed term before campaigning so a crash cannot
        # resurrect the old term. adopt_term is a no-op when equal but keeps
        # the durable contract explicit.
        {new_state, effects} = adopt_term(rpc.term, %{state | leader_id: nil})

        {:candidate, new_state, effects}
    end
  end

  defp voter?(state) do
    try do
      get_membership(state) == :voter
    rescue
      _ -> true
    catch
      _, _ -> true
    end
  end

  # ---------------------------------------------------------------------------
  # Private — query helpers
  # ---------------------------------------------------------------------------

  defp local_query_result(query_fun, state, raft_state) do
    %{
      cfg: %Config{id: id, machine: {_, mac_mod, _}, effective_machine_version: mac_ver},
      machine_state: mac_state,
      last_applied: last,
      current_term: term
    } = state

    ctx = %{index: last, term: term, machine_version: mac_ver}
    result = Machine.query(mac_mod, query_fun, mac_state, ctx)
    {raft_state, state, [{:reply, {:ok, result, id}}]}
  end

  defp consistent_query_admit(query_fun, state) do
    qr = {:query, Map.get(state, :query_from), query_fun, state.commit_index}
    peer_map = Cluster.peers(state.cfg.id, state.cluster)

    if map_size(peer_map) == 0 do
      effects = apply_consistent_queries_effects([qr], state)
      {:leader, state, effects}
    else
      {new_state, effects} = make_heartbeat_rpc_effects(qr, state)
      {:leader, new_state, effects}
    end
  end

  defp consistent_aux_admit(aux_cmd, state) do
    qr = {:aux, Map.get(state, :query_from), aux_cmd, state.commit_index}
    peer_map = Cluster.peers(state.cfg.id, state.cluster)

    if map_size(peer_map) == 0 do
      effects = apply_consistent_queries_effects([qr], state)
      {:leader, state, effects}
    else
      {new_state, effects} = make_heartbeat_rpc_effects(qr, state)
      {:leader, new_state, effects}
    end
  end

  # credo:disable-for-lines:60
  defp evaluate_consistent_heartbeats(state) do
    waiting = state.queries_waiting_heartbeats
    list = :queue.to_list(waiting)

    {remaining, state, effects} =
      Enum.reduce(list, {[], state, []}, fn {qi, qr}, {acc, st, effs} ->
        read_ci = elem(qr, 3)
        voter_total = Cluster.voter_count(st.cluster)

        count =
          Enum.count(st.cluster, fn {id, peer} ->
            is_voter =
              case peer do
                %{voter_status: %{membership: :voter}} -> true
                _ -> id == st.cfg.id and st.membership == :voter
              end

            is_voter and
              (id == st.cfg.id or Map.get(peer, :query_index, 0) >= qi)
          end)

        majority = div(voter_total, 2) + 1

        if count >= majority do
          if st.last_applied >= read_ci do
            reply_eff = process_consistent_query(qr, st)
            {acc, st, [reply_eff | effs]}
          else
            st = %{st | pending_consistent_queries: st.pending_consistent_queries ++ [qr]}
            {acc, st, effs}
          end
        else
          {[{qi, qr} | acc], st, effs}
        end
      end)

    new_waiting = :queue.from_list(Enum.reverse(remaining))
    state = %{state | queries_waiting_heartbeats: new_waiting}
    {state, effects}
  end

  defp process_pending_read_queries(%{pending_consistent_queries: []} = state, effects),
    do: {state, effects}

  defp process_pending_read_queries(state, effects) do
    {ready, rest} =
      Enum.split_with(state.pending_consistent_queries, fn qr ->
        elem(qr, 3) <= state.last_applied
      end)

    state = %{state | pending_consistent_queries: rest}
    new_effects = Enum.map(ready, &process_consistent_query(&1, state))
    {state, new_effects ++ effects}
  end

  defp transfer_leadership(target, state) do
    id = state.cfg.id

    cond do
      target == id ->
        Metrics.inc(:leadership_transfers_failed)
        {:leader, state, [{:reply, {:error, :self}}]}

      not Map.has_key?(state.cluster, target) ->
        Metrics.inc(:leadership_transfers_failed)
        {:leader, state, [{:reply, {:error, :not_member}}]}

      true ->
        peer = Map.get(state.cluster, target)

        is_voter =
          case peer do
            %{voter_status: %{membership: :voter}} -> true
            _ -> false
          end

        caught_up = Map.get(peer, :match_index, 0) >= state.commit_index

        if not is_voter or not caught_up do
          Metrics.inc(:leadership_transfers_failed)
          {:leader, state, [{:reply, {:error, :not_caught_up}}]}
        else
          Metrics.inc(:leadership_transfers_initiated)
          # send TimeoutNowRpc
          rpc = %Types.TimeoutNowRpc{term: state.current_term, leader_id: id}
          effects = [{:send_rpc, target, rpc}]
          state = Map.put(state, :transfer_leadership, target)

          {:leader, state,
           effects ++ [{:transfer_leadership_timeout, target}, {:reply, {:ok, target}}]}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Public state accessors
  # ---------------------------------------------------------------------------

  def id(%{cfg: %Config{id: id}}), do: id
  def uid(%{cfg: %Config{uid: uid}}), do: uid
  def log_id(%{cfg: %Config{log_id: log_id}}), do: log_id
  def system_config(%{cfg: %Config{system_config: sc}}), do: sc
  def leader_id(state), do: Map.get(state, :leader_id)
  def clear_leader_id(state), do: Map.put(state, :leader_id, nil)
  def current_term(%{current_term: ct}), do: ct
  def machine_version(%{cfg: %Config{machine_version: mv}}), do: mv
  def machine(%{cfg: %Config{machine: m}}), do: m

  def new?(%{log: log}), do: Log.next_index(log) == 1
  def fully_persisted?(%{log: log}), do: Log.last_written(log) == Log.last_index_term(log)

  def get_membership(%{cfg: %Config{id: id, uid: uid}, cluster: cluster} = state) do
    Cluster.get_membership(cluster, id, uid, Map.get(state, :membership, :voter))
  end

  def get_membership(cluster, %{cfg: %Config{id: id, uid: uid}} = state) when is_map(cluster) do
    Cluster.get_membership(cluster, id, uid, Map.get(state, :membership, :voter))
  end

  def peers(%{cfg: %Config{id: id}, cluster: cluster}),
    do: Cluster.peers(id, cluster)

  def update_peer(peer_id, update, %{cluster: cluster} = state) when is_map(update),
    do: %{state | cluster: Cluster.update_peer(peer_id, update, cluster)}

  # ---------------------------------------------------------------------------
  # State queries (used by ServerProc for call replies)
  # ---------------------------------------------------------------------------

  def state_query(:members, %{cluster: c}), do: Map.keys(c)
  def state_query(:members_info, %{cluster: c}), do: c
  def state_query(:leader, state), do: Map.get(state, :leader_id)
  def state_query(:machine, %{machine_state: ms}), do: ms
  def state_query(:last_applied, state), do: Map.get(state, :last_applied)

  def state_query(:initial_members, %{cluster: c} = state),
    do: Map.get(state, :initial_members, Map.keys(c))

  def state_query(:overview, state), do: overview(state)
  def state_query(_, state), do: state

  def overview(state) do
    Map.take(state, [
      :current_term,
      :commit_index,
      :last_applied,
      :cluster,
      :leader_id,
      :voted_for,
      :membership
    ])
  end

  def machine_query(query_fun, %{
        cfg: %Config{
          effective_machine_module: mac_mod,
          effective_machine_version: mac_ver
        },
        machine_state: mac_state,
        last_applied: last,
        current_term: term
      }) do
    ctx = %{index: last, term: term, machine_version: mac_ver}
    result = Machine.query(mac_mod, query_fun, mac_state, ctx)
    {{last, term}, result}
  end

  # ---------------------------------------------------------------------------
  # Persistence
  # ---------------------------------------------------------------------------

  # Persist at most every N applied entries; smaller advances ride the
  # WAL replay on restart, while clean shutdown forces a final store (11.1).
  @persist_applied_interval 64

  @doc "Persist `last_applied` to durable meta if it has advanced."
  @spec persist_last_applied(map()) :: map()
  def persist_last_applied(%{persisted_last_applied: pla, last_applied: la} = state)
      when la <= pla,
      do: state

  def persist_last_applied(%{last_applied: la0, log: log, cfg: %Config{uid: uid} = cfg} = state) do
    {lwi, _} = Log.last_written(log)
    la = min(la0, lwi)
    pla = Map.get(state, :persisted_last_applied, 0)

    if la - pla >= @persist_applied_interval do
      :ok = LogMeta.store_sync(meta_name(cfg), uid, :last_applied, la)
      publish_apply_gauges(state, la)
      Map.put(state, :persisted_last_applied, la)
    else
      state
    end
  end

  # Partial states (e.g. unit-test fixtures without apply tracking) have
  # nothing to persist.
  def persist_last_applied(state), do: state

  @doc "Force-persist `last_applied` (used on clean shutdown)."
  @spec persist_last_applied!(map()) :: map()
  def persist_last_applied!(%{last_applied: la0, log: log, cfg: %Config{uid: uid} = cfg} = state) do
    {lwi, _} = Log.last_written(log)
    la = min(la0, lwi)
    pla = Map.get(state, :persisted_last_applied, 0)

    if la > pla do
      :ok = LogMeta.store_sync(meta_name(cfg), uid, :last_applied, la)
      publish_apply_gauges(state, la)
      Map.put(state, :persisted_last_applied, la)
    else
      state
    end
  end

  defp publish_apply_gauges(%{cfg: %Config{id: id}, log: log} = state, la) do
    {lwi, _} = Log.last_written(log)
    {last_idx, _} = Log.last_index_term(log)

    snap_idx =
      case Log.snapshot_index_term(log) do
        {idx, _} when is_integer(idx) -> idx
        _ -> 0
      end

    Counters.set_gauge(id, :last_applied, la)
    Counters.set_gauge(id, :last_written_index, lwi)
    Counters.set_gauge(id, :last_index, last_idx)
    Counters.set_gauge(id, :snapshot_index, snap_idx)
    Counters.set_gauge(id, :commit_index, Map.get(state, :commit_index, 0))
    Counters.set_gauge(id, :commit_latency, Map.get(state, :commit_latency, 0))
    :ok
  end

  # ---------------------------------------------------------------------------
  # RPC construction
  # ---------------------------------------------------------------------------

  @doc "Build AppendEntries RPCs for all peers (used on heartbeat / tick)."
  @spec make_rpcs(map()) :: {map(), list()}
  def make_rpcs(state) do
    {state, _more, effs} = make_pipelined_rpc_effects(state, [], true)
    {state, effs}
  end

  @doc """
  Build replication RPCs for lagging peers only (used for ack-triggered
  catch-up). Unlike `make_rpcs/1`, up-to-date peers get no heartbeat here;
  the periodic tick covers those.
  """
  @spec make_pipeline_rpcs(map()) :: {map(), list()}
  def make_pipeline_rpcs(state) do
    {state, _more, effs} = make_pipelined_rpc_effects(state, [], false)
    {state, effs}
  end

  # ---------------------------------------------------------------------------
  # State-enter / aux / tick
  # ---------------------------------------------------------------------------

  @spec handle_state_enter(atom(), map()) :: {map(), list()}
  def handle_state_enter(
        raft_state,
        %{cfg: %Config{effective_machine_module: mac_mod}, machine_state: mac_state} = state0
      ) do
    state = become(raft_state, state0)
    effects = Machine.state_enter(mac_mod, raft_state, mac_state)
    {state, effects}
  end

  @spec handle_aux(atom(), term(), term(), map()) :: {atom(), map(), list()}
  def handle_aux(raft_state, type, _cmd, %{cfg: %Config{effective_handle_aux_fun: nil}} = state) do
    effects = if type == :cast, do: [], else: [{:reply, {:error, :aux_handler_not_implemented}}]
    {raft_state, state, effects}
  end

  def handle_aux(
        raft_state,
        type,
        cmd,
        %{
          cfg: %Config{
            effective_machine_module: mac_mod,
            effective_handle_aux_fun: {:handle_aux, 5}
          },
          aux_state: aux0
        } = state0
      ) do
    case Machine.handle_aux(mac_mod, raft_state, type, cmd, aux0, state0) do
      {:reply, reply, aux, state} -> {raft_state, put_aux(state, aux), [{:reply, reply}]}
      {:no_reply, aux, state} -> {raft_state, put_aux(state, aux), []}
      {:no_reply, aux, state, effects} -> {raft_state, put_aux(state, aux), effects}
    end
  end

  def handle_aux(
        raft_state,
        type,
        cmd,
        %{
          cfg: %Config{
            effective_machine_module: mac_mod,
            effective_handle_aux_fun: {:handle_aux, 6}
          },
          aux_state: aux0,
          log: log0,
          machine_state: mac_state0
        }
      ) do
    case Machine.handle_aux(mac_mod, raft_state, type, cmd, aux0, log0, mac_state0) do
      {:reply, reply, aux, state} -> {raft_state, put_aux(state, aux), [{:reply, reply}]}
      {:no_reply, aux, state} -> {raft_state, put_aux(state, aux), []}
      {:no_reply, aux, state, effects} -> {raft_state, put_aux(state, aux), effects}
    end
  end

  @spec tick(map()) :: list()
  def tick(%{cfg: %Config{effective_machine_module: mac_mod}, machine_state: mac_state}) do
    Machine.tick(mac_mod, :erlang.system_time(:millisecond), mac_state)
  end

  @spec log_tick(map()) :: map()
  def log_tick(%{log: log0} = state) do
    %{state | log: Log.tick(:erlang.system_time(:millisecond), log0)}
  end

  # ---------------------------------------------------------------------------
  # Cursor / checkpoint
  # ---------------------------------------------------------------------------

  @spec update_release_cursor(non_neg_integer(), term(), map(), map()) :: {map(), list()}
  def update_release_cursor(
        index,
        mac_state,
        _opts,
        %{cfg: %Config{machine: machine}, log: log0, cluster: cluster} = state
      ) do
    mac_version = index_machine_version(index, state)
    mac_mod = Machine.which_module(machine, mac_version)

    {log, effects} =
      Log.update_release_cursor(index, cluster, {mac_version, mac_mod}, mac_state, log0)

    {%{state | log: log}, effects}
  end

  @spec promote_checkpoint(non_neg_integer(), map()) :: {map(), list()}
  def promote_checkpoint(index, %{log: log0} = state) do
    {log, effects} = Log.promote_checkpoint(index, log0)
    {%{state | log: log}, effects}
  end

  @spec checkpoint(non_neg_integer(), term(), map()) :: {map(), list()}
  def checkpoint(
        index,
        mac_state,
        %{cfg: %Config{machine: machine}, log: log0, cluster: cluster} = state
      ) do
    mac_version = index_machine_version(index, state)
    mac_mod = Machine.which_module(machine, mac_version)
    {log, effects} = Log.checkpoint(index, cluster, {mac_version, mac_mod}, mac_state, log0)
    {%{state | log: log}, effects}
  end

  # ---------------------------------------------------------------------------
  # Terminate
  # ---------------------------------------------------------------------------

  @spec terminate(map(), term()) :: :ok
  def terminate(%{log: _log} = state, _reason) do
    state1 = maybe_write_recovery_checkpoint(state)
    {state2, _effects} = maybe_checkpoint(state1)
    %{log: log2} = persist_last_applied(state2)
    Log.close(log2)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Misc helpers
  # ---------------------------------------------------------------------------

  @spec fetch_term(non_neg_integer(), map()) :: {non_neg_integer() | nil, map()}
  def fetch_term(idx, %{log: log0} = state) do
    case Log.fetch_term(idx, log0) do
      {nil, log} ->
        result =
          case Log.snapshot_index_term(log) do
            {^idx, term} -> term
            _ -> nil
          end

        {result, %{state | log: log}}

      {term, log} ->
        {term, %{state | log: log}}
    end
  end

  def transform_for_partial_read(_idx, _term, {:"$usr", _, cmd, _}), do: cmd
  def transform_for_partial_read(_idx, _term, cmd), do: cmd

  # ---------------------------------------------------------------------------
  # Private — elections
  # ---------------------------------------------------------------------------

  defp call_for_election(
         :pre_vote,
         %{
           cfg: %Config{id: id, log_id: log_id, machine_version: mac_ver} = cfg,
           current_term: term
         } = state0
       ) do
    incr_counter(cfg, :pre_vote_elections, 1)
    Logger.debug("#{log_id}: pre_vote election in term #{term}")
    token = make_ref()
    peer_ids = Cluster.peer_ids(id, state0.cluster)
    {last_idx, last_term} = Log.last_index_term(state0.log)

    vote_rpcs =
      Enum.map(peer_ids, fn peer_id ->
        {peer_id,
         %Types.PreVoteRpc{
           term: term,
           token: token,
           machine_version: mac_ver,
           candidate_id: id,
           last_log_index: last_idx,
           last_log_term: last_term
         }}
      end)

    self_vote = %Types.PreVoteResult{term: term, token: token, vote_granted: true}

    # Pre-vote must not persist a vote in the current term; the durable
    # self-vote is deferred until the real candidate phase. Recording
    # voted_for here would block a legitimate candidate later (fail-closed
    # elections require persistence only when actually granting).
    state = state0

    {:pre_vote, Map.merge(state, %{leader_id: nil, votes: 0, pre_vote_token: token}),
     [{:next_event, :cast, self_vote}, {:send_vote_requests, vote_rpcs}]}
  end

  # This clause is never used - kept for API compatibility
  defp call_for_election(_type, _state), do: {:error, :invalid_election_type}

  # ---------------------------------------------------------------------------
  # Private — term management
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Private — quorum and commit advancement
  # ---------------------------------------------------------------------------

  defp evaluate_quorum(%{cfg: _cfg, commit_index: ci0} = state0, effects0) do
    %{commit_index: ci} = state = increment_commit_index(state0)
    effects = if ci > ci0, do: [{:aux, :eval} | effects0], else: effects0
    {state1, effects1} = apply_committed_entries(ci, state, effects)
    maybe_emit_pending_release_cursor(state1, effects1)
  end

  @doc """
  Advance `commit_index` to `candidate` when the entry there belongs to the
  current term. The index never moves backwards: a candidate at or below the
  current index is a no-op. Returns `{advanced?, state}`.
  """
  @spec advance_commit_index(map(), non_neg_integer()) :: {boolean(), map()}
  def advance_commit_index(%{current_term: ct, commit_index: ci0} = state0, candidate)
      when is_integer(candidate) and candidate > 0 do
    if candidate <= ci0 do
      {false, state0}
    else
      case fetch_term(candidate, state0) do
        {^ct, state} ->
          Metrics.inc(:quorum_reached)
          Metrics.inc(:commit_advanced)
          Metrics.set(:commit_index, candidate)

          case state do
            %{cfg: %{id: id}} ->
              Counters.set_gauge(id, :commit_index, candidate)
              Counters.set_gauge(id, :term, ct)

            _ ->
              :ok
          end

          {true, Map.put(state, :commit_index, candidate)}

        {_, state} ->
          # Prior-term entries can never commit by replica count alone.
          {false, state}
      end
    end
  end

  def advance_commit_index(state, _candidate), do: {false, state}

  defp increment_commit_index(%{cfg: %Config{id: id}, cluster: cluster, log: log} = state0) do
    {lwi, _} = Log.last_written(log)
    indexes = Cluster.voter_match_indexes(id, cluster, lwi)
    potential_ci = Cluster.agreed_commit(indexes)

    {_advanced?, state} = advance_commit_index(state0, potential_ci)
    state
  end

  @doc """
  Check automatic-compaction thresholds (entry count, WAL bytes, elapsed
  time) and emit a compaction trigger when one fires.

  A snapshot transfer in progress defers compaction; the next check retries
  (14.2). Firing resets the entry counter and timestamp so one trigger
  produces one snapshot (14.1).
  """
  @spec maybe_auto_compact(map(), integer()) :: {map(), list()}
  def maybe_auto_compact(%{cfg: %Config{} = cfg} = state, now_ms) do
    entry_thr = cfg.compaction_entry_threshold
    wal_thr = cfg.compaction_wal_bytes
    interval = cfg.compaction_interval_ms

    since = Map.get(state, :entries_since_compact, 0)
    last = Map.get(state, :last_auto_compact_ms, now_ms)

    due =
      (is_integer(entry_thr) and since >= entry_thr) or
        (is_integer(wal_thr) and wal_bytes(state) >= wal_thr) or
        (is_integer(interval) and now_ms - last >= interval)

    cond do
      not due ->
        {state, []}

      transfer_in_progress?(state) ->
        {state, []}

      true ->
        state =
          Map.merge(state, %{last_auto_compact_ms: now_ms, entries_since_compact: 0})

        {state, [{:next_event, :info, {:ra_log_event, :major_compaction}}]}
    end
  end

  # A snapshot transfer is in flight when this server is receiving one or
  # any peer is being sent one.
  defp transfer_in_progress?(%{snapshot_state: accept}) when not is_nil(accept), do: true

  defp transfer_in_progress?(%{cluster: cluster}) when is_map(cluster) do
    Enum.any?(cluster, fn
      {_, %{status: {:sending_snapshot, _, _}}} -> true
      _ -> false
    end)
  end

  defp transfer_in_progress?(_), do: false

  defp wal_bytes(%{log: %Log{data_dir: dir}}) when is_binary(dir) do
    case File.ls(dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.starts_with?(&1, "wal"))
        |> Enum.reduce(0, fn f, acc ->
          case File.stat(Path.join(dir, f)) do
            {:ok, %{size: s}} -> acc + s
            _ -> acc
          end
        end)

      _ ->
        0
    end
  end

  defp wal_bytes(_), do: 0

  # ---------------------------------------------------------------------------
  # Private — log application
  # ---------------------------------------------------------------------------

  @doc false
  def apply_committed(apply_to, state), do: apply_committed_entries(apply_to, state, [])

  # Apply all log entries up to `apply_to` against the state machine.
  defp apply_committed_entries(apply_to, state, effects) do
    do_apply_entries(apply_to, &apply_entry/2, %{}, effects, state)
  end

  defp do_apply_entries(
         apply_to,
         apply_fun,
         notifys0,
         effects0,
         %{
           last_applied: last_applied,
           leader_id: leader_id,
           cfg:
             %Config{
               id: id,
               machine_version: mac_ver,
               effective_machine_version: eff_mac_ver,
               effective_machine_module: mac_mod
             } = _cfg,
           machine_state: mac_state0,
           log: log0
         } = state0
       )
       when apply_to > last_applied and mac_ver >= eff_mac_ver do
    from = last_applied + 1
    {last_idx, _} = Log.last_index_term(log0)
    to = min(last_idx, apply_to)

    # Batch-level shared context (11.4): the machine version and the
    # leader check are identical for every entry in the batch, so compute
    # them once instead of re-deriving them per entry.
    shared = %{mac_ver: eff_mac_ver, leader?: is_nil(leader_id) or leader_id == id}
    fold_acc = {mac_mod, last_applied, state0, mac_state0, effects0, notifys0, nil, shared}

    {{_, applied_to, state, mac_state, effects, notifys, last_ts, _}, log} =
      Log.fold(from, to, apply_fun, fold_acc, log0)

    commit_latency =
      if last_ts, do: :erlang.system_time(:millisecond) - last_ts, else: 0

    final_effects = Effects.make_notify_effects(notifys, Enum.reverse(effects))

    # Publish per-batch apply progress (11.6): one gauge write per batch,
    # not per entry.
    case state do
      %{cfg: %{id: id}} ->
        Counters.set_gauge(id, :last_applied, applied_to)
        Counters.set_gauge(id, :commit_latency, commit_latency)

      _ ->
        :ok
    end

    state =
      Map.merge(state, %{
        last_applied: applied_to,
        log: log,
        commit_latency: commit_latency,
        machine_state: mac_state,
        entries_since_compact:
          Map.get(state, :entries_since_compact, 0) + max(applied_to - last_applied, 0)
      })

    # Automatic compaction check (14.1): entry, WAL-byte, and time
    # thresholds; deferred while a transfer is in flight (14.2). Times use
    # the monotonic clock, matching `last_auto_compact_ms`.
    {state, auto_effects} =
      maybe_auto_compact(state, System.monotonic_time(:millisecond))

    {state, final_effects ++ auto_effects}
  end

  defp do_apply_entries(_apply_to, _, notifys, effects, state) when is_list(effects) do
    {state, Effects.make_notify_effects(notifys, Enum.reverse(effects))}
  end

  # Guard: skip application when machine is being upgraded.
  defp apply_entry(
         _,
         {mod, la, %{cfg: %Config{machine_version: mv, effective_machine_version: em}} = state,
          mac_st, effects, notifys, last_ts, shared}
       )
       when mv < em,
       do: {mod, la, state, mac_st, effects, notifys, last_ts, shared}

  # User command.
  defp apply_entry(
         {idx, term, {:"$usr", cmd_meta, cmd, reply_mode}},
         {module, _la, state, mac_st, effects0, notifys0, last_ts, shared}
       ) do
    mac_ver = shared.mac_ver
    meta = build_command_meta(idx, term, mac_ver, reply_mode, cmd_meta)
    ts = Map.get(cmd_meta, :ts, last_ts)
    {next_mac_st, reply, mac_effs} = Machine.apply(module, meta, cmd, mac_st)

    machine_effects = Effects.append_machine_effects(mac_effs, effects0)

    # Only the leader owns the caller's reply alias. Followers applying the
    # same committed entry must not send a second reply to the client.
    # `leader?` is constant for the whole batch (computed once in
    # `do_apply_entries/5`), not re-derived per entry.
    {effects, notifys} =
      if shared.leader? do
        Effects.add_reply(cmd_meta, reply, reply_mode, machine_effects, notifys0)
      else
        {machine_effects, notifys0}
      end

    {module, idx, state, next_mac_st, effects, notifys, ts, shared}
  end

  # Cluster change.
  defp apply_entry(
         {idx, _term, {:"$ra_cluster_change", cmd_meta, new_cluster, reply_mode}},
         {mod, _, state0, mac_st, effects0, notifys0, last_ts, shared}
       ) do
    {effects, notifys} = Effects.add_reply(cmd_meta, :ok, reply_mode, effects0, notifys0)

    state =
      case state0 do
        %{cluster_index_term: {ci, ct}} when idx > ci and state0.current_term >= ct ->
          Logger.debug("#{state0.cfg.log_id}: applying cluster change at #{idx}")

          %{
            state0
            | cluster: new_cluster,
              membership: get_membership(new_cluster, state0),
              cluster_change_permitted: true,
              cluster_index_term: {idx, state0.current_term}
          }

        _ ->
          %{
            state0
            | cluster: new_cluster,
              membership: get_membership(new_cluster, state0),
              cluster_change_permitted: true
          }
      end

    {mod, idx, state, mac_st, effects, notifys, last_ts, shared}
  end

  # Noop / machine-version upgrade entry.
  defp apply_entry(
         {idx, term, {:noop, cmd_meta, next_mac_ver}},
         {cur_module, la,
          %{
            cfg:
              %Config{
                log_id: log_id,
                machine_version: mac_ver,
                machine: machine,
                machine_versions: mac_versions,
                effective_machine_version: old_mac_ver
              } = cfg0,
            current_term: ct
          } = state0, mac_st, effects, notifys, last_ts, shared}
       ) do
    cluster_change_perm = ct == term or state0.cluster_change_permitted

    cond do
      next_mac_ver > old_mac_ver and mac_ver >= next_mac_ver ->
        module = Machine.which_module(machine, next_mac_ver)

        cfg = %{
          cfg0
          | effective_machine_version: next_mac_ver,
            machine_versions: [{idx, next_mac_ver} | mac_versions],
            effective_machine_module: module,
            effective_handle_aux_fun: Machine.which_aux_fun(module)
        }

        state = %{state0 | cfg: cfg, cluster_change_permitted: cluster_change_perm}
        meta = build_command_meta(idx, term, mac_ver, nil, cmd_meta)

        apply_entry(
          {idx, term, {:"$usr", meta, {:machine_version, old_mac_ver, next_mac_ver}, :none}},
          {module, la, state, mac_st, effects, notifys, last_ts,
           %{shared | mac_ver: next_mac_ver}}
        )

      next_mac_ver > old_mac_ver ->
        Logger.debug("#{log_id}: unknown machine version #{next_mac_ver}")
        cfg = %{cfg0 | effective_machine_version: next_mac_ver}
        {cur_module, la, %{state0 | cfg: cfg}, mac_st, effects, notifys, last_ts, shared}

      true ->
        state = %{state0 | cluster_change_permitted: cluster_change_perm}
        {cur_module, idx, state, mac_st, effects, notifys, last_ts, shared}
    end
  end

  # Cluster delete.
  defp apply_entry(
         {idx, _, {:"$ra_cluster", cmd_meta, :delete, reply_type}},
         {module, _, state0, mac_st, effects0, notifys0, _last_ts, _shared}
       ) do
    {effects1, notifys} = Effects.add_reply(cmd_meta, :ok, reply_type, effects0, notifys0)
    eol_effects = Machine.state_enter(module, :eol, mac_st)
    not_effects = Effects.make_notify_effects(notifys, [])
    state = %{state0 | last_applied: idx, machine_state: mac_st}
    throw({:delete_and_terminate, state, eol_effects ++ not_effects ++ effects1})
  end

  # Unknown entry — just advance last_applied index.
  defp apply_entry({idx, _, _}, acc), do: put_elem(acc, 1, idx)

  # ---------------------------------------------------------------------------
  # Private — command meta construction
  # ---------------------------------------------------------------------------

  # Fast path for the common cast shape: skip the per-entry reduce when
  # there is nothing to merge (11.4).
  defp build_command_meta(idx, term, mac_ver, reply_mode, cmd_meta) when cmd_meta == %{} do
    %{index: idx, machine_version: mac_ver, term: term, reply_mode: reply_mode}
  end

  defp build_command_meta(idx, term, mac_ver, reply_mode, cmd_meta) do
    build_command_meta(idx, term, mac_ver, Map.put(cmd_meta, :reply_mode, reply_mode))
  end

  defp build_command_meta(idx, term, mac_ver, cmd_meta) do
    Enum.reduce(cmd_meta, %{index: idx, machine_version: mac_ver, term: term}, fn
      {:ts, v}, acc -> Map.put(acc, :system_time, v)
      {k, v}, acc -> Map.put(acc, k, v)
    end)
  end

  # ---------------------------------------------------------------------------
  # Private — log append (leader-side)
  # ---------------------------------------------------------------------------

  defp append_log_as_leader(cmd, %{cluster: cluster, cfg: cfg} = state, effects) do
    append_log_as_leader(cmd, cluster, cfg, state, effects)
  end

  defp append_log_as_leader({tag, _, _, _}, _, _, state, effects)
       when tag in [:"$ra_join", :"$ra_leave", :"$ra_promote", :"$ra_demote"] and
              not state.cluster_change_permitted,
       do: {:not_appended, :cluster_change_not_permitted, state, effects}

  defp append_log_as_leader(
         {:"$ra_join", from, {joining, membership}, reply_mode},
         cluster,
         _cfg,
         state,
         effects
       ) do
    if Map.has_key?(cluster, joining) do
      {:not_appended, :already_member, state, effects}
    else
      peer =
        Cluster.new_peer()
        |> Map.put(:voter_status, %{membership: membership, uid: nil})

      new_cluster = Map.put(cluster, joining, peer)
      append_cluster_change(new_cluster, from, reply_mode, state, effects)
    end
  end

  defp append_log_as_leader(
         {:"$ra_join", from, joining, reply_mode},
         cluster,
         cfg,
         state,
         effects
       ) do
    append_log_as_leader(
      {:"$ra_join", from, {joining, :voter}, reply_mode},
      cluster,
      cfg,
      state,
      effects
    )
  end

  defp append_log_as_leader(
         {:"$ra_promote", from, target, reply_mode},
         cluster,
         _cfg,
         state,
         effects
       ) do
    case Map.fetch(cluster, target) do
      :error ->
        {:not_appended, :not_member, state, effects}

      {:ok, peer} ->
        membership = get_in(peer, [:voter_status, :membership]) || :voter

        cond do
          membership == :voter ->
            {:not_appended, :already_voter, state, effects}

          Map.get(peer, :match_index, 0) < state.commit_index ->
            {:not_appended, :not_caught_up, state, effects}

          true ->
            existing_uid = get_in(peer, [:voter_status, :uid])

            new_cluster =
              put_in(cluster, [target, :voter_status], %{membership: :voter, uid: existing_uid})

            append_cluster_change(new_cluster, from, reply_mode, state, effects)
        end
    end
  end

  defp append_log_as_leader(
         {:"$ra_demote", from, target, reply_mode},
         cluster,
         _cfg,
         state,
         effects
       ) do
    case Map.fetch(cluster, target) do
      :error ->
        {:not_appended, :not_member, state, effects}

      {:ok, peer} ->
        membership = get_in(peer, [:voter_status, :membership]) || :voter

        cond do
          membership != :voter ->
            {:not_appended, :not_voter, state, effects}

          Cluster.voter_count(cluster) <= 1 ->
            {:not_appended, :last_voter, state, effects}

          true ->
            existing_uid = get_in(peer, [:voter_status, :uid])

            new_cluster =
              put_in(cluster, [target, :voter_status], %{
                membership: :non_voter,
                uid: existing_uid
              })

            append_cluster_change(new_cluster, from, reply_mode, state, effects)
        end
    end
  end

  defp append_log_as_leader(
         {:"$ra_leave", from, leaving, reply_mode},
         cluster,
         %Config{log_id: log_id},
         state,
         effects
       ) do
    case cluster do
      %{^leaving => _} ->
        new_cluster = Map.delete(cluster, leaving)
        state = %{state | cluster: new_cluster}
        append_cluster_change(new_cluster, from, reply_mode, state, effects)

      _ ->
        Logger.debug("#{log_id}: #{inspect(leaving)} not a member")
        {:not_appended, :not_member, state, effects}
    end
  end

  defp append_log_as_leader(cmd, _, _, %{log: log0, current_term: term} = state, effects) do
    next_idx = Log.next_index(log0)
    entry = {next_idx, term, cmd}

    try do
      case Log.write([entry], log0) do
        {:ok, log} ->
          {:ok, next_idx, term, %{state | log: log}, effects}

        {:error, reason} ->
          {:not_appended, reason, state, effects}
      end
    rescue
      _ -> {:not_appended, :wal_down, state, effects}
    end
  end

  # Append a homogeneous batch of user commands with a single `Log.write/2`
  # (one WAL write + one sync for the whole batch). Returns `{state, effects,
  # write_result}` where effects carry the pipelined replication work, quorum
  # evaluation, and per-command `:after_log_append` replies. On a write error
  # every caller in the batch gets an error reply and the log is untouched.
  defp append_command_batch(cmds, %{log: log0, current_term: term} = state0) do
    base = Log.next_index(log0)

    entries =
      Enum.with_index(cmds, fn cmd, i -> {base + i, term, cmd} end)

    try do
      case Log.write(entries, log0) do
        {:ok, log} ->
          {state, _, effects1} =
            make_pipelined_rpc_effects(%{state0 | log: log}, [], false)

          {state2, effects2} = evaluate_quorum(state, effects1)

          effects =
            cmds
            |> Enum.with_index(fn cmd, i -> {cmd, base + i} end)
            |> Enum.reduce(effects2, fn {cmd, idx}, acc ->
              Effects.after_log_append_reply(cmd, idx, term, acc)
            end)

          {state2, effects, :ok}

        {:error, reason} ->
          effects =
            Enum.reduce(cmds, [], fn cmd, acc ->
              Effects.append_error_reply(cmd, reason, acc)
            end)

          {state0, effects, {:error, reason}}
      end
    rescue
      _ ->
        effects =
          Enum.reduce(cmds, [], fn cmd, acc ->
            Effects.append_error_reply(cmd, :wal_down, acc)
          end)

        {state0, effects, {:error, :wal_down}}
    end
  end

  defp append_cluster_change(
         cluster,
         from,
         reply_mode,
         %{
           log: log0,
           current_term: term
         } = state,
         effects
       ) do
    cmd = {:"$ra_cluster_change", from, cluster, reply_mode}
    next_idx = Log.next_index(log0)
    entry = {next_idx, term, cmd}

    try do
      case Log.write([entry], log0) do
        {:ok, log} ->
          {:ok, next_idx, term,
           %{
             state
             | log: log,
               cluster_change_permitted: false,
               cluster_index_term: {next_idx, term}
           }, effects}

        {:error, reason} ->
          {:not_appended, reason, state, effects}
      end
    rescue
      e ->
        Logger.error("RaftEx.Domain.Server: append_cluster_change failed: #{inspect(e)}")
        {:not_appended, :wal_down, state, effects}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — RPC effects
  # ---------------------------------------------------------------------------

  defp make_pipelined_rpc_effects(state, effects, heartbeat_current?) do
    %{
      cfg: %Config{
        id: id,
        max_append_entries_rpc_batch_size: batch_size,
        max_pipeline_count: max_pipeline
      },
      commit_index: ci,
      log: log,
      cluster: cluster
    } = state

    next_log_idx = Log.next_index(log)

    # 9.5: peers servable from the log share a single ascending read for the
    # whole pass instead of one fold + reverse per peer. Peers whose next
    # index points into a compacted region keep the per-peer snapshot path.
    snap_idx =
      case Log.snapshot_index_term(log) do
        {idx, _} -> idx
        _ -> nil
      end

    servable_from = if is_integer(snap_idx), do: snap_idx + 1, else: 1

    targets =
      for {peer_id,
           %{next_index: ni, status: :normal, commit_index_sent: cis, match_index: mi} = peer0} <-
            cluster,
          peer_id != id,
          ni < next_log_idx or cis < ci,
          do: {peer_id, peer0, ni, mi}

    shared =
      case Enum.filter(targets, fn {_, _, ni, _} -> ni >= servable_from end) do
        [] ->
          nil

        servable ->
          min_ni = servable |> Enum.map(fn {_, _, ni, _} -> ni end) |> Enum.min()
          {desc, _} = Log.fold(min_ni, next_log_idx - 1, fn e, a -> [e | a] end, [], log)
          {Enum.reverse(desc), min_ni}
      end

    {state, more, effects, served} =
      Enum.reduce(targets, {state, false, effects, MapSet.new()}, fn {peer_id, peer0, ni, mi},
                                                                     {s0, more0, effs, served0} ->
        shared_for =
          case shared do
            {asc, base} when ni >= base -> {asc, ni - base}
            _ -> nil
          end

        {s, more, effs} =
          process_peer_replication(
            peer_id,
            peer0,
            ni,
            mi,
            ci,
            next_log_idx,
            more0,
            effs,
            s0,
            batch_size,
            max_pipeline,
            shared_for
          )

        {s, more, effs, MapSet.put(served0, peer_id)}
      end)

    if heartbeat_current? do
      send_heartbeats(state, more, effects, served, id, ci)
    else
      {state, more, effects}
    end
  end

  # Heartbeats for up-to-date, normal peers skipped by the replication pass.
  defp send_heartbeats(state, more, effects, served, id, ci) do
    Enum.reduce(state.cluster, {state, more, effects}, fn
      {^id, _}, acc ->
        acc

      {peer_id, %{status: :normal, next_index: ni, commit_index_sent: cis} = peer0},
      {s0, more0, effs} ->
        if not MapSet.member?(served, peer_id) and ni >= Log.next_index(s0.log) and cis >= ci do
          {s, eff} = make_heartbeat_effect(peer_id, peer0, s0)
          {s, more0, [eff | effs]}
        else
          {s0, more0, effs}
        end

      _, acc ->
        acc
    end)
  end

  # An empty AppendEntries that carries only the commit index. It advances
  # neither `next_index` nor the in-flight window.
  defp make_heartbeat_effect(peer_id, peer0, state) do
    %{log: log0, current_term: term, commit_index: ci, cfg: %Config{id: id}} = state
    {last_idx, last_term} = Log.last_index_term(log0)

    rpc = %Types.AppendEntriesRpc{
      entries: [],
      term: term,
      leader_id: id,
      prev_log_index: last_idx,
      prev_log_term: last_term,
      leader_commit: ci
    }

    Metrics.inc(:append_entries_sent)
    peer = Map.put(peer0, :commit_index_sent, ci)
    {put_peer(peer_id, peer, state), {:send_rpc, peer_id, rpc}}
  end

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  defp process_peer_replication(
         peer_id,
         peer0,
         ni,
         mi,
         ci,
         next_log_idx,
         more0,
         effs,
         s0,
         batch_size,
         max_pipeline,
         shared
       ) do
    count = Map.get(peer0, :backpressure_count, 0)

    if count > 0 do
      peer = Map.put(peer0, :backpressure_count, count - 1)
      {put_peer(peer_id, peer, s0), more0, effs}
    else
      in_flight = ni - mi - 1
      effective_batch = max(1, min(batch_size, max_pipeline - in_flight))

      if in_flight < max_pipeline do
        {new_ni, eff, s} = make_rpc_effect(peer_id, peer0, effective_batch, s0, shared)

        in_flight_entry = {ni - 1, new_ni - 1}

        peer =
          peer0
          |> Map.put(:next_index, new_ni)
          |> Map.put(:commit_index_sent, ci)
          |> Map.put(:backpressure_count, 0)
          |> Map.update(:in_flight, :queue.from_list([in_flight_entry]), fn
            q when is_list(q) -> :queue.from_list(q ++ [in_flight_entry])
            q -> :queue.in(in_flight_entry, q)
          end)

        new_more = more0 or (new_ni < next_log_idx and new_ni - mi - 1 < max_pipeline)
        {put_peer(peer_id, peer, s), new_more, [eff | effs]}
      else
        Metrics.inc(:replication_backpressure)
        peer = Map.put(peer0, :backpressure_count, 3)
        {put_peer(peer_id, peer, s0), more0, effs}
      end
    end
  end

  defp make_rpc_effect(peer_id, %{next_index: next} = _peer, batch_size, state, shared) do
    prev_idx = next - 1
    %{log: log0} = state

    Metrics.inc(:append_entries_sent)

    case shared do
      {asc_entries, offset} when is_list(asc_entries) and is_integer(offset) and offset >= 0 ->
        # 9.5: slice this peer's batch out of the pass-wide shared read.
        prev_term =
          if offset == 0 do
            elem(Log.fetch_term(prev_idx, log0), 0)
          else
            case Enum.at(asc_entries, offset - 1) do
              {_, t, _} -> t
              _ -> nil
            end
          end

        if is_integer(prev_term) do
          last_idx = Log.next_index(log0) - 1
          to = min(last_idx, prev_idx + batch_size)
          entries = Enum.slice(asc_entries, offset, max(to - prev_idx, 0))

          rpc = %Types.AppendEntriesRpc{
            entries: entries,
            term: state.current_term,
            leader_id: state.cfg.id,
            prev_log_index: prev_idx,
            prev_log_term: prev_term,
            leader_commit: state.commit_index
          }

          {to + 1, {:send_rpc, peer_id, rpc}, state}
        else
          # Shared window does not cover prev (truncated corner): per-peer.
          make_rpc_effect(peer_id, %{next_index: next}, batch_size, state, nil)
        end

      _ ->
        case Log.fetch_term(prev_idx, log0) do
          {prev_term, log} when is_integer(prev_term) ->
            make_append_entries_rpc(
              peer_id,
              prev_idx,
              prev_term,
              batch_size,
              %{state | log: log},
              []
            )

          {nil, log} ->
            %{cfg: %Config{id: id}, current_term: term} = state

            case Log.snapshot_index_term(log) do
              {^prev_idx, prev_term} ->
                make_append_entries_rpc(
                  peer_id,
                  prev_idx,
                  prev_term,
                  batch_size,
                  %{state | log: log},
                  []
                )

              {snap_idx, _} when not is_nil(snap_idx) ->
                snap_state = Log.snapshot_state(log)
                {snap_idx, {:send_snapshot, peer_id, {snap_state, id, term}}, %{state | log: log}}

              _ ->
                make_append_entries_rpc(
                  peer_id,
                  0,
                  0,
                  batch_size,
                  %{state | log: log},
                  []
                )
            end
        end
    end
  end

  defp make_append_entries_rpc(
         peer_id,
         prev_idx,
         prev_term,
         num,
         %{log: log0, current_term: term, cfg: %Config{id: id}, commit_index: ci} = state,
         entry_cache
       ) do
    {last_idx, _} = Log.last_index_term(log0)
    from = prev_idx + 1
    to = min(last_idx, prev_idx + num)
    {entries, log} = read_log_entries(from, to, entry_cache, log0)

    rpc = %Types.AppendEntriesRpc{
      entries: Enum.reverse(entries),
      term: term,
      leader_id: id,
      prev_log_index: prev_idx,
      prev_log_term: prev_term,
      leader_commit: ci
    }

    {to + 1, {:send_rpc, peer_id, rpc}, %{state | log: log}}
  end

  defp read_log_entries(from, to, [], log0),
    do: Log.fold(from, to, fn e, a -> [e | a] end, [], log0)

  defp read_log_entries(from0, to, cache, log0) do
    {from, entries0} = fold_from_cache(from0, to, cache, [])
    Log.fold(from, to, fn e, a -> [e | a] end, entries0, log0)
  end

  defp fold_from_cache(from, to, [{from, _, _} = entry | rem], acc) when from <= to,
    do: fold_from_cache(from + 1, to, rem, [entry | acc])

  defp fold_from_cache(from, _to, _cache, acc), do: {from, acc}

  # ---------------------------------------------------------------------------
  # Private — consistent queries
  # ---------------------------------------------------------------------------

  defp process_pending_consistent_queries(%{cluster_change_permitted: false} = state, effects),
    do: {state, effects}

  defp process_pending_consistent_queries(%{pending_consistent_queries: []} = state, effects),
    do: {state, effects}

  defp process_pending_consistent_queries(
         %{pending_consistent_queries: pending} = state0,
         effects0
       ) do
    Enum.reduce(pending, {%{state0 | pending_consistent_queries: []}, effects0}, fn query_ref,
                                                                                    {state,
                                                                                     effects} ->
      {new_state, new_effects} = make_heartbeat_rpc_effects(query_ref, state)
      {new_state, new_effects ++ effects}
    end)
  end

  defp make_heartbeat_rpc_effects(
         query_ref,
         %{
           query_index: qi,
           queries_waiting_heartbeats: waiting0,
           current_term: term,
           commit_index: ci,
           cfg: %Config{id: id},
           cluster: cluster
         } = state0
       ) do
    peer_map = Cluster.peers(id, cluster)

    if map_size(peer_map) == 0 do
      {state0, apply_consistent_queries_effects([query_ref], state0)}
    else
      new_qi = qi + 1
      state = %{state0 | query_index: new_qi}
      effects = heartbeat_rpc_effects(peer_map, id, term, new_qi, ci)
      waiting1 = :queue.in({new_qi, query_ref}, waiting0)
      {%{state | queries_waiting_heartbeats: waiting1}, effects}
    end
  end

  defp heartbeat_rpc_effects(peers_map, id, term, qi, commit_index) do
    Enum.flat_map(peers_map, fn
      {peer_id, %{status: :normal, query_index: pqi}} when pqi < qi ->
        [
          {:send_rpc, peer_id,
           %Types.HeartbeatRpc{
             query_index: qi,
             term: term,
             leader_id: id,
             commit_index: commit_index
           }}
        ]

      _ ->
        []
    end)
  end

  defp apply_consistent_queries_effects(query_refs, %{last_applied: la} = state) do
    Enum.map(query_refs, fn {_, _, _, read_ci} = qr ->
      true = la >= read_ci
      process_consistent_query(qr, state)
    end)
  end

  defp process_consistent_query(
         {:query, from, query_fun, _},
         %{
           cfg: %Config{id: id, machine: {_, mac_mod, _}, effective_machine_version: mac_ver},
           machine_state: mac_state,
           last_applied: last,
           current_term: term
         }
       ) do
    ctx = %{index: last, term: term, machine_version: mac_ver}
    result = Machine.query(mac_mod, query_fun, mac_state, ctx)
    {:reply, from, {:ok, result, id}}
  end

  defp process_consistent_query({:aux, from, aux_cmd, _}, state) do
    {_, _state, effects} = handle_aux(:leader, :call, aux_cmd, state)

    Enum.map(effects, fn
      {:reply, reply} -> {:reply, from, reply}
      eff -> eff
    end)
  end

  # ---------------------------------------------------------------------------
  # Private — release cursor / checkpoint
  # ---------------------------------------------------------------------------

  defp maybe_emit_pending_release_cursor(
         %{pending_release_cursor: {index, mac_state, conds}} = state,
         effects
       ) do
    if check_release_cursor_conditions(conds, state) do
      {state1, new_effs} =
        do_update_release_cursor(index, mac_state, Map.delete(state, :pending_release_cursor))

      {state1, new_effs ++ effects}
    else
      {state, effects}
    end
  end

  defp maybe_emit_pending_release_cursor(state, effects), do: {state, effects}

  defp maybe_checkpoint(%{log: log0, last_applied: idx, cfg: %Config{machine: machine}} = state)
       when idx > 0 do
    snap_idx =
      case Log.snapshot_index_term(log0) do
        {i, _} -> i
        nil -> 0
      end

    if idx > snap_idx do
      mac_ver = index_machine_version(idx, state)
      mac_mod = Machine.which_module(machine, mac_ver)

      {log, effects} =
        Log.checkpoint(idx, state.cluster, {mac_ver, mac_mod}, state.machine_state, log0)

      {%{state | log: log}, effects}
    else
      {state, []}
    end
  end

  defp maybe_checkpoint(state), do: {state, []}

  defp do_update_release_cursor(
         index,
         mac_state,
         %{cfg: %Config{machine: machine}, log: log0, cluster: cluster} = state
       ) do
    mac_version = index_machine_version(index, state)
    mac_mod = Machine.which_module(machine, mac_version)

    {log, effects} =
      Log.update_release_cursor(index, cluster, {mac_version, mac_mod}, mac_state, log0)

    {%{state | log: log}, effects}
  end

  defp check_release_cursor_conditions(conds, state) do
    Enum.all?(conds, &check_release_cursor_condition(&1, state))
  end

  defp check_release_cursor_condition({:written, written_idx}, %{log: log}) do
    {lwi, _} = Log.last_written(log)
    written_idx <= lwi
  end

  defp check_release_cursor_condition(:no_snapshot_sends, %{cluster: cluster}) do
    not Enum.any?(Map.values(cluster), fn
      %{status: {:sending_snapshot, _, _}} -> true
      _ -> false
    end)
  end

  # ---------------------------------------------------------------------------
  # Private — state transitions
  # ---------------------------------------------------------------------------

  defp become(:leader, %{cluster: cluster, log: log0} = state) do
    log = Log.release_resources(map_size(cluster), :sequential, log0)

    state
    |> Map.put(:log, log)
    |> Map.put(:cluster_change_permitted, false)
    |> Map.put(:became_leader_at, System.monotonic_time(:millisecond))
  end

  defp become(:follower, %{log: log0} = state) do
    log = Log.release_resources(1, :random, log0)
    cluster1 = Cluster.reset_peer_statuses(state.cluster)

    state
    |> Map.put(:log, log)
    |> Map.put(:cluster, cluster1)
    |> Map.put(:leader_id, nil)
    |> Map.put(:transfer_leadership, nil)
    |> Map.delete(:became_leader_at)
  end

  defp become(_, state), do: state

  @doc """
  Check-quorum: verify a majority of voters has been heard from within
  `timeout_ms`. Returns `{:leader, state, []}` when quorum holds or during
  the grace period after election, and `{:follower, state, []}` when the
  majority is lost, so a partitioned leader steps down.
  """
  @spec check_quorum(map(), integer(), non_neg_integer()) :: {atom(), map(), list()}
  def check_quorum(%{cfg: %Config{id: self_id}, cluster: cluster} = state, now_ms, timeout_ms) do
    became_at = Map.get(state, :became_leader_at, now_ms)

    if now_ms - became_at < timeout_ms do
      {:leader, state, []}
    else
      voters = Cluster.voters(cluster)
      total = length(voters)
      needed = div(total, 2) + 1

      self_voter? =
        case Map.get(cluster, self_id) do
          %{voter_status: %{membership: :voter}} -> true
          %{voter_status: %{membership: _}} -> false
          _ -> true
        end

      fresh_self = if self_voter?, do: 1, else: 0

      fresh_peers =
        Enum.count(voters, fn id ->
          id != self_id and
            case Map.get(cluster, id) do
              %{last_ack: last} when is_integer(last) and last != 0 ->
                diff = now_ms - last
                diff >= 0 and diff <= timeout_ms

              _ ->
                false
            end
        end)

      if fresh_self + fresh_peers >= needed do
        {:leader, state, []}
      else
        new_state =
          state
          |> Map.put(:leader_id, nil)
          |> Map.put(:transfer_leadership, nil)
          |> Map.delete(:became_leader_at)

        {:follower, new_state, []}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Private — cluster scan (recovery)
  # ---------------------------------------------------------------------------

  defp cluster_scan_fun({idx, term, {:"$ra_cluster_change", _meta, new_cluster, _}}, {_, state0}) do
    Logger.debug(
      "#{state0.cfg.log_id}: cluster_scan applying cluster change " <>
        "to #{inspect(Map.keys(new_cluster))}"
    )

    {idx,
     %{
       state0
       | cluster: new_cluster,
         membership: get_membership(new_cluster, state0),
         cluster_change_permitted: true,
         cluster_index_term: {idx, term}
     }}
  end

  defp cluster_scan_fun({idx, _, _}, {_, state}), do: {idx, state}

  # ---------------------------------------------------------------------------
  # Private — misc
  # ---------------------------------------------------------------------------

  # defp build_append_entries_reply(term, success, %{log: log} = state) do
  #   {lwi, lwt} = Log.last_written(log)
  #   {last_idx, _} = Log.last_index_term(log)

  #   %Types.AppendEntriesReply{
  #     term: term,
  #     success: success,
  #     next_index: last_idx + 1,
  #     last_index: lwi,
  #     last_term: lwt
  #   }
  # end

  defp wal_down_condition(_msg, %{log: log} = state),
    do: {Log.can_write?(log), state}

  defp index_machine_version(idx, %{cfg: %Config{machine_versions: versions}}) do
    do_index_machine_version(idx, versions)
  end

  defp do_index_machine_version(idx, [{m_idx, v} | _]) when idx >= m_idx, do: v
  defp do_index_machine_version(idx, [_ | rest]), do: do_index_machine_version(idx, rest)
  defp do_index_machine_version(idx, []), do: raise("machine_version_not_known for #{idx}")

  defp init_from_snapshot(log, machine, id, initial_nodes, initial_mac_ver) do
    case Log.recover_snapshot(log) do
      nil ->
        mac_state = Machine.init(machine, elem(id, 0), initial_mac_ver)
        cluster = Cluster.new(id, initial_nodes)
        {cluster, initial_mac_ver, mac_state, {0, 0}}

      mac_st ->
        {idx, term} =
          case Log.snapshot_index_term(log) do
            {i, t} -> {i, t}
            nil -> {0, 0}
          end

        cluster =
          case Log.snapshot_cluster(log) do
            snap_cluster when is_map(snap_cluster) and map_size(snap_cluster) > 0 ->
              restored =
                Enum.reduce(snap_cluster, %{}, fn {member_id, %{voter_status: vs}}, acc ->
                  Map.put(acc, member_id, Cluster.new_peer() |> Map.put(:voter_status, vs))
                end)

              if Map.has_key?(restored, id) do
                restored
              else
                Map.put(restored, id, Cluster.new_peer())
              end

            _ ->
              Cluster.new(id, initial_nodes)
          end

        {cluster, initial_mac_ver, mac_st, {idx, term}}
    end
  end

  defp normalise_machine({:simple, fun, s}),
    do: {:machine, RaftEx.Domain.Machine.Simple, %{simple_fun: fun, initial_state: s}}

  defp normalise_machine({:module, mod, args}),
    do: {:machine, mod, args}

  defp normalise_machine(m), do: m

  defp put_aux(state, aux), do: Map.put(state, :aux_state, aux)

  defp put_peer(peer_id, peer, %{cluster: cluster} = state),
    do: %{state | cluster: Map.put(cluster, peer_id, peer)}

  def meta_name(%Config{system_config: %{names: %{log_meta: name}}}), do: name
  def meta_name(%{names: %{log_meta: name}}), do: name

  def meta_name(%Config{system_config: %{names: names}}) when is_map(names) do
    names[:log_meta] || :ra_log_meta
  end

  def meta_name(%{names: names}) when is_map(names) do
    names[:log_meta] || :ra_log_meta
  end

  def meta_name(_), do: :ra_log_meta

  defp maybe_write_recovery_checkpoint(state), do: state

  defp maybe_recover_from_recovery_checkpoint(last_applied, _commit_index, _snap_state, state),
    do: {last_applied, state}

  defp incr_counter(%Config{id: id} = _cfg, field, n) do
    Counters.inc(id, field, n)
  end
end
