defmodule RaftEx.Server do
  @moduledoc """
  Core Raft server logic: leader election, log replication, and state machine
  application.

  This module is a pure-functional layer — it receives messages, computes the
  next state and a list of *effects*, and returns them to the caller
  (`RaftEx.ServerProc`), which owns the OTP process and actually executes the
  effects.

  ## Server state map keys

  | Key                          | Description                                      |
  |------------------------------|--------------------------------------------------|
  | `:cfg`                       | `RaftEx.Server.Config` struct (immutable mostly) |
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
  | `:log`                       | `RaftEx.Log` state                               |
  | `:machine_state`             | User state machine state                         |
  | `:aux_state`                 | Auxiliary state machine state                    |
  | `:query_index`               | Monotonic index for consistent query tracking    |
  | `:queries_waiting_heartbeats`| Queue of `{query_index, query_ref}` tuples       |
  | `:pending_consistent_queries`| Queries queued awaiting cluster-change perm      |
  """

  require Logger

  alias RaftEx.Server.{Cluster, Config, Effects}
  alias RaftEx.{Log, Machine, Types}

  # ---------------------------------------------------------------------------
  # Initialisation
  # ---------------------------------------------------------------------------

  @doc "Build the initial server state from a configuration map."
  @spec init(map()) :: map()
  def init(%{id: id, uid: uid, cluster_name: _cn, initial_members: initial_nodes,
             log_init_args: log_init_args, machine: machine_conf} = config) do
    system_config = Map.get(config, :system_config, RaftEx.System.default_config())
    log_id        = Map.get(config, :friendly_name, inspect(id))
    machine       = normalise_machine(machine_conf)
    snap_module   = Machine.snapshot_module(machine)
    counter       = Map.get(config, :counter)

    log = Log.init(Map.merge(log_init_args, %{
      snapshot_module:        snap_module,
      machine:                machine,
      system_config:          system_config,
      uid:                    uid,
      counter:                counter,
      log_id:                 log_id,
      initial_access_pattern: :sequential
    }))

    meta_name      = meta_name(system_config)
    current_term   = LogMeta.fetch(meta_name, uid, :current_term, 0)
    last_applied   = LogMeta.fetch(meta_name, uid, :last_applied, 0)
    voted_for      = LogMeta.fetch(meta_name, uid, :voted_for, nil)
    latest_mac_ver = Machine.version(machine)
    initial_mac_ver = min(latest_mac_ver, Map.get(config, :initial_machine_version, 0))

    {cluster, eff_mac_ver, mac_state, {snap_idx, _} = snap_idx_term} =
      init_from_snapshot(log, machine, id, initial_nodes, initial_mac_ver)

    mac_mod       = Machine.which_module(machine, eff_mac_ver)
    commit_index  = max(last_applied, snap_idx)

    cfg = %Config{
      id:                         id,
      uid:                        uid,
      log_id:                     log_id,
      metrics_key:                Map.get(config, :metrics_key, RaftEx.Lib.ra_server_id_to_local_name(id)),
      machine:                    machine,
      machine_version:            latest_mac_ver,
      machine_versions:           [{snap_idx, eff_mac_ver}],
      effective_machine_version:  eff_mac_ver,
      effective_machine_module:   mac_mod,
      effective_handle_aux_fun:   Machine.which_aux_fun(mac_mod),
      max_pipeline_count:         Map.get(config, :max_pipeline_count, 4096),
      counter:                    counter,
      system_config:              system_config
    }

    %{
      cfg:                           cfg,
      leader_id:                     nil,
      current_term:                  current_term,
      cluster:                       cluster,
      cluster_change_permitted:      false,
      cluster_index_term:            snap_idx_term,
      voted_for:                     voted_for,
      membership:                    :voter,
      commit_index:                  commit_index,
      last_applied:                  snap_idx,
      persisted_last_applied:        last_applied,
      log:                           log,
      machine_state:                 mac_state,
      aux_state:                     Machine.init_aux(mac_mod, elem(id, 0)),
      query_index:                   0,
      queries_waiting_heartbeats:    :queue.new(),
      pending_consistent_queries:    []
    }
  end

  # ---------------------------------------------------------------------------
  # Recovery
  # ---------------------------------------------------------------------------

  @doc "Replay committed log entries onto the state machine during startup."
  @spec recover(map()) :: map()
  def recover(%{cfg: %Config{log_id: log_id, machine_version: mac_ver},
                commit_index: commit_index,
                last_applied: last_applied,
                log: log} = state0) do
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
    from_scan       = commit_index + 1
    {to_scan, _}    = Log.last_index_term(log1)

    {{last_scanned, state3}, log2} =
      Log.fold(from_scan, to_scan, &cluster_scan_fun/2, {commit_index, state2}, log1, :return)

    state =
      if last_scanned < to_scan do
        {:ok, log3} = Log.set_last_index(last_scanned, log2)
        %{state3 | log: log3}
      else
        state3
      end

    Map.put(state, :commit_latency, 0)
  end

  # ---------------------------------------------------------------------------
  # Leader message handlers
  # ---------------------------------------------------------------------------

  @doc false
  @spec handle_leader(term(), map()) :: {atom(), map(), list()}
  def handle_leader({:command, cmd}, %{cfg: cfg} = state0) do
    incr_counter(cfg, :commands, 1)

    case append_log_as_leader(cmd, state0, []) do
      {:ok, idx, term, state1, effects0} ->
        force = match?({:noop, _, _}, cmd)
        {state, _, effects1} = make_pipelined_rpc_effects(state1, effects0, force)
        effects = Effects.after_log_append_reply(cmd, idx, term, effects1)
        {:leader, state, effects}

      {:not_appended, :wal_down, state, effects0} ->
        condition = %{predicate_fun: &wal_down_condition/2, transition_to: :leader}
        effects   = Effects.append_error_reply(cmd, :wal_down, effects0)
        {:await_condition, Map.put(state, :condition, condition), effects}

      {:not_appended, reason, state, effects0} ->
        Logger.warning("#{state.cfg.log_id} command NOT appended: #{reason}")
        {:leader, state, Effects.append_error_reply(cmd, reason, effects0)}
    end
  end

  def handle_leader({:ra_log_event, {:written, _, _} = evt}, %{log: log0} = state0) do
    {log, effects0}   = Log.handle_event(evt, log0)
    {state1, effects1} = evaluate_quorum(%{state0 | log: log}, effects0)
    {state, effects}   = process_pending_consistent_queries(state1, effects1)
    {:leader, state, [{:next_event, :info, :pipeline_rpcs} | effects]}
  end

  def handle_leader({:ra_log_event, evt}, %{log: log0} = state) do
    {log, effects} = Log.handle_event(evt, log0)
    {:leader, %{state | log: log}, effects}
  end

  def handle_leader(_msg, state), do: {:leader, state, []}

  # ---------------------------------------------------------------------------
  # Follower message handlers
  # ---------------------------------------------------------------------------

  @doc false
  @spec handle_follower(term(), map()) :: {atom(), map(), list()}
  def handle_follower(%Types.AppendEntriesRpc{} = rpc, %{cfg: %Config{id: id}} = state0) do
    reply   = build_append_entries_reply(rpc.term, true, state0)
    effects = [{:cast, rpc.leader_id, {id, reply}}]
    {:follower, state0, effects}
  end

  def handle_follower(:election_timeout, state) do
    call_for_election(:pre_vote, state)
  end

  def handle_follower(_msg, state), do: {:follower, state, []}

  # ---------------------------------------------------------------------------
  # Public state accessors
  # ---------------------------------------------------------------------------

  def id(%{cfg: %Config{id: id}}),                             do: id
  def uid(%{cfg: %Config{uid: uid}}),                          do: uid
  def log_id(%{cfg: %Config{log_id: log_id}}),                 do: log_id
  def system_config(%{cfg: %Config{system_config: sc}}),       do: sc
  def leader_id(state),                                        do: Map.get(state, :leader_id)
  def clear_leader_id(state),                                  do: Map.put(state, :leader_id, nil)
  def current_term(%{current_term: ct}),                       do: ct
  def machine_version(%{cfg: %Config{machine_version: mv}}),   do: mv
  def machine(%{cfg: %Config{machine: m}}),                    do: m

  def is_new?(%{log: log}),              do: Log.next_index(log) == 1
  def is_fully_persisted?(%{log: log}),  do: Log.last_written(log) == Log.last_index_term(log)

  def get_membership(%{cfg: %Config{id: id, uid: uid}, cluster: cluster} = state) do
    Cluster.get_membership(cluster, id, uid, Map.get(state, :membership, :voter))
  end

  def get_membership(cluster, %{cfg: %Config{id: id, uid: uid}} = state) do
    Cluster.get_membership(cluster, id, uid, Map.get(state, :membership, :voter))
  end

  def peers(%{cfg: %Config{id: id}, cluster: cluster}),
    do: Cluster.peers(id, cluster)

  def update_peer(peer_id, update, %{cluster: cluster} = state) when is_map(update),
    do: %{state | cluster: Cluster.update_peer(peer_id, update, cluster)}

  # ---------------------------------------------------------------------------
  # State queries (used by ServerProc for call replies)
  # ---------------------------------------------------------------------------

  def state_query(:members,      %{cluster: c}),     do: Map.keys(c)
  def state_query(:members_info, %{cluster: c}),     do: c
  def state_query(:leader,       state),             do: Map.get(state, :leader_id)
  def state_query(:machine,      %{machine_state: ms}), do: ms
  def state_query(:last_applied, state),             do: Map.get(state, :last_applied)
  def state_query(:initial_members, %{cluster: c}), do: Map.keys(c)
  def state_query(:overview,     state),             do: overview(state)
  def state_query(_,             state),             do: state

  def overview(state) do
    Map.take(state, [:current_term, :commit_index, :last_applied, :cluster,
                     :leader_id, :voted_for, :membership])
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
    ctx    = %{index: last, term: term, machine_version: mac_ver}
    result = Machine.query(mac_mod, query_fun, mac_state, ctx)
    {{last, term}, result}
  end

  # ---------------------------------------------------------------------------
  # Persistence
  # ---------------------------------------------------------------------------

  @doc "Persist `last_applied` to durable meta if it has advanced."
  @spec persist_last_applied(map()) :: map()
  def persist_last_applied(%{persisted_last_applied: pla, last_applied: la} = state)
      when la <= pla,
      do: state

  def persist_last_applied(%{last_applied: la0, log: log, cfg: %Config{uid: uid} = cfg} = state) do
    {lwi, _} = Log.last_written(log)
    la       = min(la0, lwi)
    pla      = Map.get(state, :persisted_last_applied, 0)

    if la > pla do
      :ok = LogMeta.store(meta_name(cfg), uid, :last_applied, la)
      Map.put(state, :persisted_last_applied, la)
    else
      state
    end
  end

  # ---------------------------------------------------------------------------
  # RPC construction
  # ---------------------------------------------------------------------------

  @doc "Build AppendEntries RPCs for all peers (used on heartbeat / tick)."
  @spec make_rpcs(map()) :: {map(), list()}
  def make_rpcs(state) do
    Enum.reduce(peers(state), {state, []}, fn {peer_id, peer}, {s0, effs} ->
      {_, eff, s} = make_rpc_effect(peer_id, peer, 1, s0)
      {s, [eff | effs]}
    end)
  end

  # ---------------------------------------------------------------------------
  # State-enter / aux / tick
  # ---------------------------------------------------------------------------

  @spec handle_state_enter(atom(), map()) :: {map(), list()}
  def handle_state_enter(raft_state, %{cfg: %Config{effective_machine_module: mac_mod},
                                       machine_state: mac_state} = state0) do
    state   = become(raft_state, state0)
    effects = Machine.state_enter(mac_mod, raft_state, mac_state)
    {state, effects}
  end

  @spec handle_aux(atom(), term(), term(), map()) :: {atom(), map(), list()}
  def handle_aux(raft_state, type, _cmd, %{cfg: %Config{effective_handle_aux_fun: nil}} = state) do
    effects = if type == :cast, do: [], else: [{:reply, {:error, :aux_handler_not_implemented}}]
    {raft_state, state, effects}
  end

  def handle_aux(raft_state, type, cmd,
                 %{cfg: %Config{effective_machine_module: mac_mod,
                                effective_handle_aux_fun: {:handle_aux, 5}},
                   aux_state: aux0} = state0) do
    case Machine.handle_aux(mac_mod, raft_state, type, cmd, aux0, state0) do
      {:reply, reply, aux, state}          -> {raft_state, put_aux(state, aux), [{:reply, reply}]}
      {:no_reply, aux, state}              -> {raft_state, put_aux(state, aux), []}
      {:no_reply, aux, state, effects}     -> {raft_state, put_aux(state, aux), effects}
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
  def update_release_cursor(index, mac_state, opts, %{cfg: %Config{machine: machine},
                                                        log: log0, cluster: cluster} = state) do
    mac_version = index_machine_version(index, state)
    mac_mod     = Machine.which_module(machine, mac_version)
    {log, effects} = Log.update_release_cursor(index, cluster, {mac_version, mac_mod}, mac_state, log0)
    {%{state | log: log}, effects}
  end

  @spec promote_checkpoint(non_neg_integer(), map()) :: {map(), list()}
  def promote_checkpoint(index, %{log: log0} = state) do
    {log, effects} = Log.promote_checkpoint(index, log0)
    {%{state | log: log}, effects}
  end

  @spec checkpoint(non_neg_integer(), term(), map()) :: {map(), list()}
  def checkpoint(index, mac_state, %{cfg: %Config{machine: machine},
                                      log: log0, cluster: cluster} = state) do
    mac_version = index_machine_version(index, state)
    mac_mod     = Machine.which_module(machine, mac_version)
    {log, effects} = Log.checkpoint(index, cluster, {mac_version, mac_mod}, mac_state, log0)
    {%{state | log: log}, effects}
  end

  # ---------------------------------------------------------------------------
  # Terminate
  # ---------------------------------------------------------------------------

  @spec terminate(map(), term()) :: :ok
  def terminate(%{log: log} = state, _reason) do
    state1 = maybe_write_recovery_checkpoint(state)
    %{log: log2} = persist_last_applied(state1)
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
            _            -> nil
          end
        {result, %{state | log: log}}

      {term, log} ->
        {term, %{state | log: log}}
    end
  end

  def transform_for_partial_read(_idx, _term, {:"$usr", _, cmd, _}), do: cmd
  def transform_for_partial_read(_idx, _term, cmd),                  do: cmd

  # ---------------------------------------------------------------------------
  # Private — elections
  # ---------------------------------------------------------------------------

  defp call_for_election(:pre_vote, %{cfg: %Config{id: id, log_id: log_id,
                                                    machine_version: mac_ver} = cfg,
                                       current_term: term} = state0) do
    incr_counter(cfg, :pre_vote_elections, 1)
    Logger.debug("#{log_id}: pre_vote election in term #{term}")
    token      = make_ref()
    peer_ids   = Cluster.peer_ids(id, state0.cluster)
    {last_idx, last_term} = Log.last_index_term(state0.log)

    vote_rpcs =
      Enum.map(peer_ids, fn peer_id ->
        {peer_id, %Types.PreVoteRpc{
          term: term, token: token, machine_version: mac_ver,
          candidate_id: id, last_log_index: last_idx, last_log_term: last_term
        }}
      end)

    self_vote = %Types.PreVoteResult{term: term, token: token, vote_granted: true}
    state     = update_term_and_voted_for(term, id, state0)

    {:pre_vote,
     Map.merge(state, %{leader_id: nil, votes: 0, pre_vote_token: token}),
     [{:next_event, :cast, self_vote}, {:send_vote_requests, vote_rpcs}]}
  end

  defp call_for_election(:candidate, %{cfg: %Config{id: id, log_id: log_id} = cfg,
                                        current_term: current_term} = state0) do
    incr_counter(cfg, :elections, 1)
    new_term   = current_term + 1
    Logger.debug("#{log_id}: election in term #{new_term}")
    peer_ids   = Cluster.peer_ids(id, state0.cluster)
    {last_idx, last_term} = Log.last_index_term(state0.log)

    vote_rpcs =
      Enum.map(peer_ids, fn peer_id ->
        {peer_id, %Types.RequestVoteRpc{
          term: new_term, candidate_id: id,
          last_log_index: last_idx, last_log_term: last_term
        }}
      end)

    self_vote = %Types.RequestVoteResult{term: new_term, vote_granted: true}
    state     = update_term_and_voted_for(new_term, id, state0)

    {:candidate,
     Map.merge(state, %{leader_id: nil, votes: 0}),
     [{:next_event, :cast, self_vote}, {:send_vote_requests, vote_rpcs}]}
  end

  # ---------------------------------------------------------------------------
  # Private — term management
  # ---------------------------------------------------------------------------

  defp update_term_and_voted_for(term, voted_for,
                                  %{cfg: %Config{uid: uid} = cfg,
                                    current_term: cur_term} = state) do
    if term == cur_term and voted_for == Map.get(state, :voted_for) do
      state
    else
      meta = meta_name(cfg)
      LogMeta.store(meta, uid, :current_term, term)
      LogMeta.store_sync(meta, uid, :voted_for, voted_for)
      incr_counter(cfg, :term_and_voted_for_updates, 1)

      state
      |> Map.merge(%{current_term: term, voted_for: voted_for})
      |> Map.update!(:cluster, &Cluster.reset_query_indexes/1)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — quorum and commit advancement
  # ---------------------------------------------------------------------------

  defp evaluate_quorum(%{cfg: cfg, commit_index: ci0} = state0, effects0) do
    %{commit_index: ci} = state = increment_commit_index(state0)
    effects = if ci > ci0, do: [{:aux, :eval} | effects0], else: effects0
    {state1, effects1} = apply_committed_entries(ci, state, effects)
    maybe_emit_pending_release_cursor(state1, effects1)
  end

  defp increment_commit_index(%{current_term: ct,
                                 cfg: %Config{id: id},
                                 cluster: cluster,
                                 log: log} = state0) do
    {lwi, _}    = Log.last_written(log)
    indexes     = Cluster.voter_match_indexes(id, cluster, lwi)
    potential_ci = Cluster.agreed_commit(indexes)

    case fetch_term(potential_ci, state0) do
      {^ct, state} -> Map.put(state, :commit_index, potential_ci)
      {_, state}   -> state
    end
  end

  # ---------------------------------------------------------------------------
  # Private — log application
  # ---------------------------------------------------------------------------

  # Apply all log entries up to `apply_to` against the state machine.
  defp apply_committed_entries(apply_to, state, effects) do
    do_apply_entries(apply_to, &apply_entry/2, %{}, effects, state)
  end

  defp do_apply_entries(apply_to, apply_fun, notifys0, effects0,
                        %{last_applied: last_applied,
                          cfg: %Config{machine_version: mac_ver,
                                       effective_machine_version: eff_mac_ver,
                                       effective_machine_module: mac_mod} = cfg,
                          machine_state: mac_state0,
                          log: log0} = state0)
       when apply_to > last_applied and mac_ver >= eff_mac_ver do
    from      = last_applied + 1
    {last_idx, _} = Log.last_index_term(log0)
    to        = min(last_idx, apply_to)

    fold_acc  = {mac_mod, last_applied, state0, mac_state0, effects0, notifys0, nil}

    {{_, applied_to, state, mac_state, effects, notifys, last_ts}, log} =
      Log.fold(from, to, apply_fun, fold_acc, log0)

    commit_latency =
      if last_ts, do: :erlang.system_time(:millisecond) - last_ts, else: 0

    final_effects = Effects.make_notify_effects(notifys, Enum.reverse(effects))

    {%{state |
       last_applied:    applied_to,
       log:             log,
       commit_latency:  commit_latency,
       machine_state:   mac_state},
     final_effects}
  end

  defp do_apply_entries(_apply_to, _, notifys, effects, state) when is_list(effects) do
    {state, Effects.make_notify_effects(notifys, Enum.reverse(effects))}
  end

  # Guard: skip application when machine is being upgraded.
  defp apply_entry(_, {mod, la, %{cfg: %Config{machine_version: mv, effective_machine_version: em}} = state,
                        mac_st, effects, notifys, last_ts})
       when mv < em,
    do: {mod, la, state, mac_st, effects, notifys, last_ts}

  # User command.
  defp apply_entry({idx, term, {:"$usr", cmd_meta, cmd, reply_mode}},
                   {module, _la, state, mac_st, effects0, notifys0, last_ts}) do
    mac_ver            = state.cfg.effective_machine_version
    meta               = build_command_meta(idx, term, mac_ver, reply_mode, cmd_meta)
    ts                 = Map.get(cmd_meta, :ts, last_ts)
    {next_mac_st, reply, mac_effs} = Machine.apply(module, meta, cmd, mac_st)

    {effects, notifys} =
      Effects.add_reply(
        cmd_meta, reply, reply_mode,
        Effects.append_machine_effects(mac_effs, effects0),
        notifys0
      )

    {module, idx, state, next_mac_st, effects, notifys, ts}
  end

  # Cluster change.
  defp apply_entry({idx, _term, {:"$ra_cluster_change", cmd_meta, new_cluster, reply_mode}},
                   {mod, _, state0, mac_st, effects0, notifys0, last_ts}) do
    {effects, notifys} = Effects.add_reply(cmd_meta, :ok, reply_mode, effects0, notifys0)

    state =
      case state0 do
        %{cluster_index_term: {ci, ct}} when idx > ci and state0.current_term >= ct ->
          Logger.debug("#{state0.cfg.log_id}: applying cluster change at #{idx}")
          %{state0 |
            cluster:                  new_cluster,
            membership:               get_membership(new_cluster, state0),
            cluster_change_permitted: true,
            cluster_index_term:       {idx, state0.current_term}}

        _ ->
          %{state0 | cluster_change_permitted: true}
      end

    {mod, idx, state, mac_st, effects, notifys, last_ts}
  end

  # Noop / machine-version upgrade entry.
  defp apply_entry({idx, term, {:noop, cmd_meta, next_mac_ver}},
                   {cur_module, la, %{cfg: %Config{
                     log_id: log_id, machine_version: mac_ver, machine: machine,
                     machine_versions: mac_versions,
                     effective_machine_version: old_mac_ver} = cfg0,
                     current_term: ct} = state0,
                    mac_st, effects, notifys, last_ts}) do
    cluster_change_perm = ct == term or state0.cluster_change_permitted

    cond do
      next_mac_ver > old_mac_ver and mac_ver >= next_mac_ver ->
        module = Machine.which_module(machine, next_mac_ver)
        cfg    = %{cfg0 |
          effective_machine_version:  next_mac_ver,
          machine_versions:           [{idx, next_mac_ver} | mac_versions],
          effective_machine_module:   module,
          effective_handle_aux_fun:   Machine.which_aux_fun(module)
        }
        state  = %{state0 | cfg: cfg, cluster_change_permitted: cluster_change_perm}
        meta   = build_command_meta(idx, term, mac_ver, nil, cmd_meta)
        apply_entry(
          {idx, term, {:"$usr", meta, {:machine_version, old_mac_ver, next_mac_ver}, :none}},
          {module, la, state, mac_st, effects, notifys, last_ts}
        )

      next_mac_ver > old_mac_ver ->
        Logger.debug("#{log_id}: unknown machine version #{next_mac_ver}")
        cfg   = %{cfg0 | effective_machine_version: next_mac_ver}
        {cur_module, la, %{state0 | cfg: cfg}, mac_st, effects, notifys, last_ts}

      true ->
        state = %{state0 | cluster_change_permitted: cluster_change_perm}
        {cur_module, idx, state, mac_st, effects, notifys, last_ts}
    end
  end

  # Cluster delete.
  defp apply_entry({idx, _, {:"$ra_cluster", cmd_meta, :delete, reply_type}},
                   {module, _, state0, mac_st, effects0, notifys0, _last_ts}) do
    {effects1, notifys} = Effects.add_reply(cmd_meta, :ok, reply_type, effects0, notifys0)
    eol_effects  = Machine.state_enter(module, :eol, mac_st)
    not_effects  = Effects.make_notify_effects(notifys, [])
    state        = %{state0 | last_applied: idx, machine_state: mac_st}
    throw({:delete_and_terminate, state, eol_effects ++ not_effects ++ effects1})
  end

  # Unknown entry — just advance last_applied index.
  defp apply_entry({idx, _, _}, acc), do: put_elem(acc, 1, idx)

  # ---------------------------------------------------------------------------
  # Private — command meta construction
  # ---------------------------------------------------------------------------

  defp build_command_meta(idx, term, mac_ver, nil, cmd_meta),
    do: build_command_meta(idx, term, mac_ver, cmd_meta)

  defp build_command_meta(idx, term, mac_ver, reply_mode, cmd_meta),
    do: build_command_meta(idx, term, mac_ver, Map.put(cmd_meta, :reply_mode, reply_mode))

  defp build_command_meta(idx, term, mac_ver, cmd_meta) do
    Enum.reduce(cmd_meta, %{index: idx, machine_version: mac_ver, term: term}, fn
      {:ts, v}, acc -> Map.put(acc, :system_time, v)
      {k, v}, acc   -> Map.put(acc, k, v)
    end)
  end

  # ---------------------------------------------------------------------------
  # Private — log append (leader-side)
  # ---------------------------------------------------------------------------

  defp append_log_as_leader(cmd, %{cluster: cluster, cfg: cfg} = state, effects) do
    append_log_as_leader(cmd, cluster, cfg, state, effects)
  end

  defp append_log_as_leader({tag, _, _, _}, _, _, state, effects)
       when tag in [:"$ra_join", :"$ra_leave"] and not state.cluster_change_permitted,
    do: {:not_appended, :cluster_change_not_permitted, state, effects}

  defp append_log_as_leader({:"$ra_leave", from, leaving, reply_mode},
                             cluster, %Config{log_id: log_id}, state, effects) do
    case cluster do
      %{^leaving => _} ->
        new_cluster = Map.delete(cluster, leaving)
        append_cluster_change(new_cluster, from, reply_mode, state, effects)

      _ ->
        Logger.debug("#{log_id}: #{inspect(leaving)} not a member")
        {:not_appended, :not_member, state, effects}
    end
  end

  defp append_log_as_leader(cmd, _, _, %{log: log0, current_term: term} = state, effects) do
    next_idx = Log.next_index(log0)

    try do
      log = Log.append({next_idx, term, cmd}, log0)
      {:ok, next_idx, term, %{state | log: log}, effects}
    rescue
      _ -> {:not_appended, :wal_down, state, effects}
    end
  end

  defp append_cluster_change(cluster, from, reply_mode,
                              %{log: log0, cluster: prev_cluster,
                                cluster_index_term: {prev_ci, prev_ct},
                                current_term: term} = state,
                              effects) do
    cmd      = {:"$ra_cluster_change", from, cluster, reply_mode}
    next_idx = Log.next_index(log0)

    try do
      log = Log.append({next_idx, term, cmd}, log0)
      {:ok, next_idx, term,
       %{state |
         log:                      log,
         cluster:                  cluster,
         cluster_change_permitted: false,
         cluster_index_term:       {next_idx, term},
         previous_cluster:         {prev_ci, prev_ct, prev_cluster}},
       effects}
    rescue
      _ -> {:not_appended, :wal_down, state, effects}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — RPC effects
  # ---------------------------------------------------------------------------

  defp make_pipelined_rpc_effects(state, effects, force \\ false) do
    %{cfg: %Config{id: id, max_append_entries_rpc_batch_size: batch_size,
                   max_pipeline_count: max_pipeline},
      commit_index: ci,
      log: log,
      cluster: cluster} = state

    next_log_idx = Log.next_index(log)

    Enum.reduce(cluster, {state, false, effects}, fn
      {^id, _}, acc ->
        acc

      {peer_id,
       %{next_index: ni, status: :normal, commit_index_sent: cis, match_index: mi} = peer0},
      {s0, more0, effs}
      when ni < next_log_idx or cis < ci ->
        in_flight        = ni - mi - 1
        effective_batch  = max(1, min(batch_size, max_pipeline - in_flight))

        if in_flight < max_pipeline or force do
          {new_ni, eff, s} = make_rpc_effect(peer_id, peer0, effective_batch, s0, [])
          peer     = %{peer0 | next_index: new_ni, commit_index_sent: ci}
          new_more = more0 or (new_ni < next_log_idx and new_ni - mi - 1 < max_pipeline)
          {put_peer(peer_id, peer, s), new_more, [eff | effs]}
        else
          {s0, more0, effs}
        end

      _, acc ->
        acc
    end)
  end

  defp make_rpc_effect(peer_id, %{next_index: next} = peer, batch_size, state, cache \\ []) do
    prev_idx = next - 1
    %{log: log0} = state

    case Log.fetch_term(prev_idx, log0) do
      {prev_term, log} when is_integer(prev_term) ->
        make_append_entries_rpc(peer_id, prev_idx, prev_term, batch_size, %{state | log: log}, cache)

      {nil, log} ->
        %{cfg: %Config{id: id}, current_term: term} = state

        case Log.snapshot_index_term(log) do
          {^prev_idx, prev_term} ->
            make_append_entries_rpc(peer_id, prev_idx, prev_term, batch_size, %{state | log: log}, cache)

          {snap_idx, _} ->
            snap_state = Log.snapshot_state(log)
            {snap_idx, {:send_snapshot, peer_id, {snap_state, id, term}}, %{state | log: log}}
        end
    end
  end

  defp make_append_entries_rpc(peer_id, prev_idx, prev_term, num,
                                %{log: log0, current_term: term,
                                  cfg: %Config{id: id}, commit_index: ci} = state,
                                entry_cache) do
    {last_idx, _} = Log.last_index_term(log0)
    from          = prev_idx + 1
    to            = min(last_idx, prev_idx + num)
    {entries, log} = read_log_entries(from, to, entry_cache, log0)

    rpc = %Types.AppendEntriesRpc{
      entries:        Enum.reverse(entries),
      term:           term,
      leader_id:      id,
      prev_log_index: prev_idx,
      prev_log_term:  prev_term,
      leader_commit:  ci
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

  defp process_pending_consistent_queries(%{pending_consistent_queries: pending} = state0, effects0) do
    Enum.reduce(pending, {%{state0 | pending_consistent_queries: []}, effects0},
      fn query_ref, {state, effects} ->
        {new_state, new_effects} = make_heartbeat_rpc_effects(query_ref, state)
        {new_state, new_effects ++ effects}
      end)
  end

  defp make_heartbeat_rpc_effects(query_ref,
                                   %{query_index: qi, queries_waiting_heartbeats: waiting0,
                                     current_term: term, cfg: %Config{id: id},
                                     cluster: cluster} = state0) do
    peer_map = Cluster.peers(id, cluster)

    if map_size(peer_map) == 0 do
      {state0, apply_consistent_queries_effects([query_ref], state0)}
    else
      new_qi   = qi + 1
      state    = %{state0 | query_index: new_qi}
      effects  = heartbeat_rpc_effects(peer_map, id, term, new_qi)
      waiting1 = :queue.in({new_qi, query_ref}, waiting0)
      {%{state | queries_waiting_heartbeats: waiting1}, effects}
    end
  end

  defp heartbeat_rpc_effects(peers_map, id, term, qi) do
    Enum.flat_map(peers_map, fn
      {peer_id, %{status: :normal, query_index: pqi}} when pqi < qi ->
        [{:send_rpc, peer_id, %Types.HeartbeatRpc{query_index: qi, term: term, leader_id: id}}]

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

  defp process_consistent_query({:query, from, query_fun, _},
                                  %{cfg: %Config{id: id, machine: {_, mac_mod, _},
                                                 effective_machine_version: mac_ver},
                                    machine_state: mac_state,
                                    last_applied: last,
                                    current_term: term}) do
    ctx    = %{index: last, term: term, machine_version: mac_ver}
    result = Machine.query(mac_mod, query_fun, mac_state, ctx)
    {:reply, from, {:ok, result, id}}
  end

  defp process_consistent_query({:aux, from, aux_cmd, _}, _state),
    do: {:next_event, {:call, from}, {:aux_command, {:"$wrap_reply", aux_cmd}}}

  # ---------------------------------------------------------------------------
  # Private — release cursor / checkpoint
  # ---------------------------------------------------------------------------

  defp maybe_emit_pending_release_cursor(
         %{pending_release_cursor: {index, mac_state, conds}} = state, effects) do
    if check_release_cursor_conditions(conds, state) do
      {state1, new_effs} =
        do_update_release_cursor(index, mac_state, Map.delete(state, :pending_release_cursor))
      {state1, new_effs ++ effects}
    else
      {state, effects}
    end
  end

  defp maybe_emit_pending_release_cursor(state, effects), do: {state, effects}

  defp do_update_release_cursor(index, mac_state,
                                  %{cfg: %Config{machine: machine},
                                    log: log0, cluster: cluster} = state) do
    mac_version = index_machine_version(index, state)
    mac_mod     = Machine.which_module(machine, mac_version)
    {log, effects} = Log.update_release_cursor(index, cluster, {mac_version, mac_mod}, mac_state, log0)
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
    log      = Log.release_resources(map_size(cluster), :sequential, log0)
    %{state | log: log, cluster_change_permitted: false}
  end

  defp become(:follower, %{log: log0} = state) do
    log      = Log.release_resources(1, :random, log0)
    cluster1 = Cluster.reset_peer_statuses(state.cluster)
    %{state | log: log, cluster: cluster1}
  end

  defp become(_, state), do: state

  # ---------------------------------------------------------------------------
  # Private — cluster scan (recovery)
  # ---------------------------------------------------------------------------

  defp cluster_scan_fun({idx, term, {:"$ra_cluster_change", _meta, new_cluster, _}}, {_, state0}) do
    Logger.debug(
      "#{state0.cfg.log_id}: cluster_scan applying cluster change " <>
      "to #{inspect(Map.keys(new_cluster))}"
    )
    {idx, %{state0 |
            cluster:                  new_cluster,
            membership:               get_membership(new_cluster, state0),
            cluster_change_permitted: true,
            cluster_index_term:       {idx, term}}}
  end

  defp cluster_scan_fun({idx, _, _}, {_, state}), do: {idx, state}

  # ---------------------------------------------------------------------------
  # Private — misc
  # ---------------------------------------------------------------------------

  defp build_append_entries_reply(term, success, %{log: log} = state) do
    {lwi, lwt}    = Log.last_written(log)
    {last_idx, _} = Log.last_index_term(log)

    %Types.AppendEntriesReply{
      term:       term,
      success:    success,
      next_index: last_idx + 1,
      last_index: lwi,
      last_term:  lwt
    }
  end

  defp wal_down_condition(_msg, %{log: log} = state),
    do: {Log.can_write?(log), state}

  defp index_machine_version(idx, %{cfg: %Config{machine_versions: versions}}) do
    do_index_machine_version(idx, versions)
  end

  defp do_index_machine_version(idx, [{m_idx, v} | _]) when idx >= m_idx, do: v
  defp do_index_machine_version(idx, [_ | rest]),   do: do_index_machine_version(idx, rest)
  defp do_index_machine_version(idx, []),           do: raise("machine_version_not_known for #{idx}")

  defp init_from_snapshot(log, machine, id, initial_nodes, initial_mac_ver) do
    case Log.recover_snapshot(log) do
      nil ->
        mac_state = Machine.init(machine, elem(id, 0), initial_mac_ver)
        cluster   = Cluster.new(id, initial_nodes)
        {cluster, initial_mac_ver, mac_state, {0, 0}}

      {%{index: idx, term: term, cluster: cluster_nodes, machine_version: mac_ver}, mac_st} ->
        cluster = Cluster.new(id, cluster_nodes)
        {cluster, mac_ver, mac_st, {idx, term}}
    end
  end

  defp normalise_machine({:simple, fun, s}),
    do: {:machine, RaftEx.MachineSimple, %{simple_fun: fun, initial_state: s}}
  defp normalise_machine({:module, mod, args}),
    do: {:machine, mod, args}
  defp normalise_machine(m), do: m

  defp put_aux(state, aux), do: Map.put(state, :aux_state, aux)

  defp put_peer(peer_id, peer, %{cluster: cluster} = state),
    do: %{state | cluster: Map.put(cluster, peer_id, peer)}

  defp meta_name(%Config{system_config: %{names: %{log_meta: name}}}), do: name
  defp meta_name(%{names: %{log_meta: name}}),                          do: name

  defp maybe_write_recovery_checkpoint(state), do: state

  defp maybe_recover_from_recovery_checkpoint(last_applied, _commit_index, _snap_state, state),
    do: {last_applied, state}

  defp incr_counter(%Config{counter: cnt}, field, n) when not is_nil(cnt) do
    # :counters.add would go here when counter integration is added
    _ = {cnt, field, n}
    :ok
  end

  defp incr_counter(_, _, _), do: :ok

  # Alias for internal brevity
  defp LogMeta, do: RaftEx.LogMeta
end
