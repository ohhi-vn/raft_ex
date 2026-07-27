defmodule RaftEx.ServerProc do
  @moduledoc """
  Gen_statem process managing a RaftEx server lifecycle.
  This is a partial implementation showing the structure.
  Full implementation would mirror ra_server_proc.erl closely.
  """

  @behaviour :gen_statem

  require Logger

  @default_election_mult 5
  @tick_interval_ms 1_000
  @default_await_condition_timeout 30_000
  @install_snap_rpc_timeout 120_000

  defstruct [
    :conf,
    :server_state,
    :monitors,
    :pending_commands,
    :leader_monitor,
    :leader_last_seen,
    :low_priority_commands,
    :election_timeout_set,
    :pending_notifys,
    :pending_queries,
    :commit_rate,
    :election_context
  ]

  # Public API

  def start_link(%{id: id} = config) do
    name = RaftEx.Lib.ra_server_id_to_local_name(id)
    :gen_statem.start_link({:local, name}, __MODULE__, config, [])
  end

  def command(server_loc, cmd, timeout) do
    leader_call(server_loc, {:command, cmd}, timeout)
  end

  def cast_command(server_id, cmd) do
    :gen_statem.cast(server_id, {:command, cmd})
  end

  def query(server_loc, query_fun, :local, options, timeout) when map_size(options) == 0 do
    statem_call(server_loc, {:local_query, query_fun}, timeout)
  end

  def query(server_loc, query_fun, :local, options, timeout) do
    statem_call(server_loc, {:local_query, query_fun, options}, timeout)
  end

  def query(server_loc, query_fun, :leader, options, timeout) when map_size(options) == 0 do
    leader_call(server_loc, {:local_query, query_fun}, timeout)
  end

  def query(server_loc, query_fun, :leader, options, timeout) do
    leader_call(server_loc, {:local_query, query_fun, options}, timeout)
  end

  def query(server_loc, query_fun, :consistent, _options, timeout) do
    leader_call(server_loc, {:consistent_query, query_fun}, timeout)
  end

  def query(server_loc, aux_cmd, :consistent_aux, _options, timeout) do
    leader_call(server_loc, {:consistent_aux, aux_cmd}, timeout)
  end

  def state_query(server_loc, spec, timeout) do
    leader_call(server_loc, {:state_query, spec}, timeout)
  end

  def local_state_query(server_loc, spec, timeout) do
    local_call(server_loc, {:state_query, spec}, timeout)
  end

  def trigger_election(server_id, timeout) do
    :gen_statem.call(server_id, :trigger_election, timeout)
  end

  def transfer_leadership(server_id, target, timeout) do
    leader_call(server_id, {:transfer_leadership, target}, timeout)
  end

  def ping(server_id, timeout) do
    safe_call(server_id, :ping, timeout)
  end

  # :gen_statem callbacks

  @impl :gen_statem
  def callback_mode, do: [:state_functions, :state_enter]

  @impl :gen_statem
  def init(config) do
    {:ok, :post_init, config, [{:next_event, :internal, :go}]}
  end

  # State: post_init
  def post_init(:enter, _old_state, state), do: {:keep_state, state, []}

  def post_init(:internal, :go, config) do
    state = do_init(config)
    {:next_state, :recover, state, [{:next_event, :internal, :go}]}
  end

  # State: recover
  def recover(:enter, old_state, state0) do
    {state, actions} = handle_enter(:recover, old_state, state0)
    {:keep_state, state, actions}
  end

  def recover(:internal, :go, %{server_state: ss0} = state) do
    ss = RaftEx.Server.recover(ss0)
    next_state(:recovered, %{state | server_state: ss}, [{:next_event, :internal, :next}])
  end

  def recover(_, _, state) do
    {:keep_state, state, [{:postpone, true}]}
  end

  # State: recovered
  def recovered(:enter, old_state, state0) do
    {state, actions} = handle_enter(:recovered, old_state, state0)
    :ok = record_cluster_change(state)
    {:keep_state, state, actions}
  end

  def recovered(:internal, :next, state) do
    :erlang.garbage_collect()
    next_state(:follower, state, set_tick_timer(state, []))
  end

  # State: leader (partial — full impl mirrors ra_server_proc.erl)
  def leader(:enter, old_state, state0) do
    {state, actions} = handle_enter(:leader, old_state, state0)
    :ok = record_cluster_change(state)

    ss = Map.put(state.server_state, :cluster_change_permitted, true)
    state = %{state | server_state: ss, election_timeout_set: false}

    cluster_name = state.conf[:cluster_name]
    leader_id = RaftEx.Server.id(state.server_state)
    members = Map.keys(state.server_state.cluster)

    if cluster_name do
      RaftEx.Leaderboard.record(cluster_name, leader_id, members)
    end

    {:keep_state, state, actions}
  end

  def leader({:call, from}, :trigger_election, state) do
    {:keep_state, state, [{:reply, from, {:error, :already_leader}}]}
  end

  def leader({:call, from}, :ping, state) do
    {:keep_state, state, [{:reply, from, {:pong, :leader}}]}
  end

  def leader({:call, from}, {:state_query, spec}, state) do
    reply = {:ok, do_state_query(spec, state), id(state)}
    {:keep_state, state, [{:reply, from, reply}]}
  end

  def leader(:_, :tick_timeout, state) do
    {state1, rpc_effs} = make_rpcs(state)
    effects = RaftEx.Server.tick(state1.server_state)
    ss = RaftEx.Server.log_tick(state1.server_state)

    {state2, actions} =
      handle_effects(:leader, rpc_effs ++ effects ++ [{:aux, :tick}], :cast, %{
        state1
        | server_state: ss
      })

    {:keep_state, state2, set_tick_timer(state2, actions)}
  end

  def leader({:call, from}, {:leader_call, {:command, cmd_usr}}, state0)
      when is_tuple(cmd_usr) and elem(cmd_usr, 0) == :"$usr" do
    {:"$usr", data, reply_mode} = cmd_usr
    cmd = {:"$usr", %{from: from}, data, reply_mode}
    leader({:call, from}, {:command, cmd}, state0)
  end

  def leader({:call, from}, {:leader_call, {:command, cmd}}, state0)
      when is_tuple(cmd) and elem(cmd, 0) in [:"$ra_join", :"$ra_leave"] do
    {tag, data, reply_mode} = cmd
    cmd = {tag, %{from: from}, data, reply_mode}
    leader({:call, from}, {:command, cmd}, state0)
  end

  def leader({:call, from}, {:leader_call, msg}, state0) do
    leader({:call, from}, msg, state0)
  end

  def leader(evt_type, msg, state0) do
    {from_pid, inner_msg} = unwrap_from(msg)

    state =
      if from_pid,
        do: put_in(state0, [:server_state, :rpc_from], pid_to_server_id(from_pid)),
        else: state0

    case handle_leader(inner_msg, state) do
      {:leader, state1, effects} ->
        {state, actions} = handle_effects(:leader, effects, evt_type, state1)
        {:keep_state, state, actions}

      {:stop, state1, effects} ->
        {state, _} = handle_effects(:leader, effects, evt_type, state1)
        {:stop, :normal, state}

      {:await_condition, state1, effects} ->
        {state, actions} = handle_effects(:leader, effects, evt_type, state1)
        next_state(:await_condition, state, actions)

      {next_state, state1, effects} ->
        {state, actions} = handle_effects(:leader, effects, evt_type, state1)
        next_state(next_state, state, actions)
    end
  end

  # State: follower (partial)
  def follower(:enter, old_state, state0) do
    {state, actions0} = handle_enter(:follower, old_state, state0)

    actions =
      if RaftEx.Server.is_new?(state.server_state) do
        actions0
      else
        {_state2, actions2} = set_election_timeout(state, actions0)
        actions2
      end

    leader_id = RaftEx.Server.leader_id(state.server_state)
    cluster_name = state.conf[:cluster_name]

    if leader_id && cluster_name do
      members = Map.keys(state.server_state.cluster)
      RaftEx.Leaderboard.record(cluster_name, leader_id, members)
    end

    {:keep_state, state, actions}
  end

  def follower({:call, from}, :ping, state) do
    {:keep_state, state, [{:reply, from, {:pong, :follower}}]}
  end

  def follower({:call, from}, :trigger_election, %{server_state: ss0} = state0) do
    state = %{state0 | server_state: RaftEx.Server.clear_leader_id(ss0)}
    next_state(:candidate, state, [{:reply, from, :ok}])
  end

  def follower({:call, from}, {:leader_call, _msg}, state0) do
    leader_id = RaftEx.Server.leader_id(state0.server_state)

    if leader_id do
      {:keep_state, state0, [{:reply, from, {:redirect, leader_id}}]}
    else
      {:keep_state, state0, [{:reply, from, {:error, :no_leader}}]}
    end
  end

  def follower(evt_type, msg, state0) do
    {from_pid, inner_msg} = unwrap_from(msg)

    state =
      if from_pid,
        do: put_in(state0, [:server_state, :rpc_from], pid_to_server_id(from_pid)),
        else: state0

    case handle_follower(inner_msg, state) do
      {:follower, state1, effects} ->
        {state, actions} = handle_effects(:follower, effects, evt_type, state1)
        {:keep_state, state, actions}

      {:pre_vote, state1, effects} ->
        {state, actions} = handle_effects(:follower, effects, evt_type, state1)
        next_state(:pre_vote, state, actions)

      {:await_condition, state1, effects} ->
        {state, actions} = handle_effects(:follower, effects, evt_type, state1)
        next_state(:await_condition, state, actions)

      {next_s, state1, effects} ->
        {state, actions} = handle_effects(:follower, effects, evt_type, state1)
        next_state(next_s, state, actions)
    end
  end

  # State: pre_vote
  def pre_vote(:enter, old_state, state0) do
    {state, actions0} = handle_enter(:pre_vote, old_state, state0)

    {context, election_effects} =
      RaftEx.Server.Election.start_election(:pre_vote, state.server_state)

    state = %{state | election_context: context}
    {s, actions} = handle_effects(:pre_vote, election_effects, :cast, state, actions0)
    {s2, timeouts} = set_election_timeout(s, [])
    {:keep_state, s2, actions ++ timeouts}
  end

  def pre_vote({:call, from}, :trigger_election, %{server_state: ss0} = state0) do
    state = %{state0 | server_state: RaftEx.Server.clear_leader_id(ss0)}
    next_state(:candidate, state, [{:reply, from, :ok}])
  end

  def pre_vote({:call, from}, :ping, state) do
    {:keep_state, state, [{:reply, from, {:pong, :pre_vote}}]}
  end

  def pre_vote(:state_timeout, :election_timeout, state0) do
    Logger.info("RaftEx.ServerProc: Pre-vote election timeout, restarting election")
    {state, actions} = handle_election_timeout(:pre_vote, state0)
    next_state(:pre_vote, state, actions)
  end

  def pre_vote({:cast, {from, reply}}, :pre_vote_reply, state0) do
    handle_pre_vote_reply(reply, from, state0)
  end

  def pre_vote(evt_type, msg, state0) do
    {from_pid, inner_msg} = unwrap_from(msg)

    state =
      if from_pid,
        do: put_in(state0, [:server_state, :rpc_from], pid_to_server_id(from_pid)),
        else: state0

    case handle_follower(inner_msg, state) do
      {:pre_vote, state1, effects} ->
        {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
        {:keep_state, state, actions}

      {:follower, state1, effects} ->
        {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
        next_state(:follower, state, actions)

      {:candidate, state1, effects} ->
        {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
        next_state(:candidate, state, actions)

      {next_s, state1, effects} ->
        {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
        next_state(next_s, state, actions)
    end
  end

  # State: candidate
  def candidate(:enter, old_state, state0) do
    {state, actions} = handle_enter(:candidate, old_state, state0)

    {context, election_effects} =
      RaftEx.Server.Election.start_election(:candidate, state.server_state)

    {state1, election_actions} =
      handle_effects(:candidate, election_effects, :cast, %{state | election_context: context})

    all_actions = actions ++ election_actions

    if MapSet.size(context.votes_received) > div(context.total_voters, 2) do
      {:keep_state, state1, all_actions ++ [{:state_timeout, 0, :become_leader}]}
    else
      {state2, prepended} = set_election_timeout(state1, all_actions)
      {:keep_state, state2, prepended}
    end
  end

  def candidate({:call, from}, :trigger_election, %{server_state: ss0} = state0) do
    state = %{state0 | server_state: RaftEx.Server.clear_leader_id(ss0)}
    next_state(:candidate, state, [{:reply, from, :ok}])
  end

  def candidate({:call, from}, {:leader_call, _msg}, state) do
    {:keep_state, state, [{:reply, from, {:error, :no_leader}}]}
  end

  def candidate({:call, from}, :ping, state) do
    {:keep_state, state, [{:reply, from, {:pong, :candidate}}]}
  end

  def candidate(:state_timeout, :election_timeout, state0) do
    Logger.info("RaftEx.ServerProc: Candidate election timeout, restarting election")
    {state, actions} = handle_election_timeout(:candidate, state0)
    next_state(:candidate, state, actions)
  end

  def candidate({:cast, {from, reply}}, :vote_reply, state0) do
    handle_vote_reply(reply, from, state0)
  end

  def candidate(:internal, :election_won, state0) do
    Logger.info("RaftEx.ServerProc: Transitioning to leader after winning election")
    {:next_state, :leader, state0, []}
  end

  def candidate(:state_timeout, :become_leader, state0) do
    Logger.info("RaftEx.ServerProc: Becoming leader after winning election")
    leader_id = RaftEx.Server.id(state0.server_state)
    cluster_name = state0.conf[:cluster_name]

    if cluster_name do
      members = Map.keys(state0.server_state.cluster)
      RaftEx.Leaderboard.record(cluster_name, leader_id, members)
    end

    {:next_state, :leader, state0, []}
  end

  def candidate(evt_type, msg, state0) do
    {from_pid, inner_msg} = unwrap_from(msg)

    state =
      if from_pid,
        do: put_in(state0, [:server_state, :rpc_from], pid_to_server_id(from_pid)),
        else: state0

    case handle_follower(inner_msg, state) do
      {:candidate, state1, effects} ->
        {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
        {:keep_state, state, actions}

      {:follower, state1, effects} ->
        {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
        next_state(:follower, state, actions)

      {:pre_vote, state1, effects} ->
        {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
        next_state(:pre_vote, state, actions)

      {next_s, state1, effects} ->
        {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
        next_state(next_s, state, actions)
    end
  end

  def receive_snapshot(:enter, _, state), do: {:keep_state, state, []}
  def receive_snapshot(_, _, state), do: {:keep_state, state, []}

  def await_condition(:enter, _, state), do: {:keep_state, state, []}
  def await_condition(_, _, state), do: {:keep_state, state, []}

  def terminating_leader(:enter, _, state), do: {:keep_state, state, []}
  def terminating_leader(_, _, state), do: {:keep_state, state, []}

  def terminating_follower(:enter, _, state), do: {:keep_state, state, []}
  def terminating_follower(_, _, state), do: {:keep_state, state, []}

  @impl :gen_statem
  def handle_event(_evt_type, _content, state_name, state) do
    Logger.warning("handle_event unknown in state #{state_name}")
    {:next_state, state_name, state}
  end

  @impl :gen_statem
  def terminate(reason, _state_name, %{server_state: _ss} = _state) do
    Logger.debug("ra_server_proc: terminating with #{inspect(reason)}")
    :ok
  end

  def terminate(_, _, _), do: :ok

  @impl :gen_statem
  def code_change(_old_vsn, state_name, state, _extra), do: {:ok, state_name, state}

  # ---- private helpers -----------------------------------------------

  defp leader_call(server_loc, msg, timeout) do
    statem_call(server_loc, {:leader_call, msg}, timeout)
  end

  defp local_call(server_loc, msg, timeout) do
    statem_call(server_loc, {:local_call, msg}, timeout)
  end

  defp statem_call(server_ids, msg, timeout) when is_list(server_ids) do
    multi_statem_call(server_ids, msg, [], timeout)
  end

  defp statem_call(server_id, msg, timeout) do
    case safe_call(server_id, msg, timeout) do
      {:redirect, leader} -> statem_call(leader, msg, timeout)
      {:wrap_reply, reply} -> {:ok, reply, server_id}
      {:error, _} = e -> e
      :timeout -> {:timeout, server_id}
      reply -> reply
    end
  end

  defp multi_statem_call([server_id | rest], msg, errs, timeout) do
    case statem_call(server_id, msg, timeout) do
      {tag, info} = e
      when tag == :timeout or (tag == :error and info in [:noproc, :nodedown, :shutdown]) ->
        if rest == [] do
          {:error, {:no_more_servers_to_try, [e | errs]}}
        else
          multi_statem_call(rest, msg, [e | errs], timeout)
        end

      reply ->
        reply
    end
  end

  defp safe_call(server_id, msg, timeout) do
    try do
      :gen_statem.call(server_id, msg, timeout)
    catch
      :exit, {:timeout, _} -> :timeout
      :exit, {:noproc, _} -> {:error, :noproc}
      :exit, {{:nodedown, _}, _} -> {:error, :nodedown}
      :exit, {:shutdown, _} -> {:error, :shutdown}
      :exit, {reason, _} -> {:error, reason}
    end
  end

  defp do_init(%{id: id, cluster_name: cluster_name} = config) do
    key = RaftEx.Lib.ra_server_id_to_local_name(id)
    true = :ets.insert(:ra_state, {key, :init, :unknown})
    Process.flag(:trap_exit, true)

    server_state = RaftEx.Server.init(config)

    if system = config[:system] do
      uid = config[:uid]
      RaftEx.Directory.register_name(system, uid, self(), self(), key, cluster_name)
    end

    sys_cfg = RaftEx.Server.system_config(server_state)
    broadcast_time = Map.get(config, :broadcast_time, Map.get(sys_cfg, :broadcast_time, 100))

    %__MODULE__{
      conf: %{
        log_id: RaftEx.Server.log_id(server_state),
        cluster_name: cluster_name,
        name: key,
        broadcast_time: broadcast_time,
        tick_timeout: Map.get(config, :tick_timeout, @tick_interval_ms),
        await_condition_timeout:
          Map.get(config, :await_condition_timeout, @default_await_condition_timeout),
        install_snap_rpc_timeout:
          Map.get(config, :install_snap_rpc_timeout, @install_snap_rpc_timeout),
        counter: Map.get(config, :counter)
      },
      server_state: server_state,
      monitors: RaftEx.Monitors.init(),
      pending_commands: [],
      leader_monitor: nil,
      election_timeout_set: false,
      pending_notifys: %{},
      pending_queries: [],
      commit_rate: {RaftEx.LeakyIntegrator.new(@tick_interval_ms * 6), 0, 0.0}
    }
  end

  defp handle_enter(raft_state, _old_state, %{conf: %{name: name}, server_state: ss0} = state) do
    membership = RaftEx.Server.get_membership(ss0)
    true = :ets.insert(:ra_state, {name, raft_state, membership})
    {ss, effects} = RaftEx.Server.handle_state_enter(raft_state, ss0)
    handle_effects(raft_state, effects, :cast, %{state | server_state: ss})
  end

  defp handle_leader(msg, %{server_state: ss0} = state0) do
    try do
      {next_state, ss, effects} = RaftEx.Server.handle_leader(msg, ss0)
      state = %{state0 | server_state: RaftEx.Server.persist_last_applied(ss)}
      {next_state, state, effects}
    catch
      :throw, {next_state, ss, effects} when is_atom(next_state) ->
        state = %{state0 | server_state: RaftEx.Server.persist_last_applied(ss)}
        {next_state, state, effects}
    end
  end

  defp handle_follower(msg, %{server_state: ss0} = state0) do
    {next_state, ss, effects} = RaftEx.Server.handle_follower(msg, ss0)
    state = %{state0 | server_state: RaftEx.Server.persist_last_applied(ss)}
    {next_state, state, effects}
  end

  defp handle_pre_vote_reply(
         reply,
         {from_pid, _ref},
         %{server_state: ss0, election_context: ctx0} = state0
       ) do
    # Look up peer_id from the mapping stored when RPC was sent
    peer_id = Process.get({:election_peer, from_pid})

    {should_start_election, ctx, effects} =
      RaftEx.Server.Election.handle_pre_vote_reply(reply, ctx0, ss0, peer_id)

    state = %{state0 | election_context: ctx}

    cond do
      should_start_election ->
        Logger.info("RaftEx.ServerProc: Pre-vote succeeded, transitioning to candidate")
        {state1, actions} = handle_effects(:pre_vote, effects, :cast, state)
        next_state(:candidate, state1, actions)

      true ->
        {state1, actions} = handle_effects(:pre_vote, effects, :cast, state)
        {:keep_state, state1, actions}
    end
  end

  defp handle_vote_reply(
         reply,
         {from_pid, _ref},
         %{server_state: ss0, election_context: ctx0} = state0
       ) do
    # Look up peer_id from the mapping stored when RPC was sent
    peer_id = Process.get({:election_peer, from_pid})

    {result, ctx, effects} = RaftEx.Server.Election.handle_vote_reply(reply, ctx0, ss0, peer_id)

    state = %{state0 | election_context: ctx}

    case result do
      :won ->
        Logger.info("RaftEx.ServerProc: Won election, becoming leader")

        {state1, actions} =
          handle_effects(:candidate, effects ++ [{:become, :leader}], :cast, state)

        next_state(:leader, state1, actions)

      :lost ->
        Logger.info("RaftEx.ServerProc: Lost election, stepping down to follower")
        {state1, actions} = handle_effects(:candidate, effects, :cast, state)
        next_state(:follower, state1, actions)

      :ongoing ->
        {state1, actions} = handle_effects(:candidate, effects, :cast, state)
        {:keep_state, state1, actions}
    end
  end

  defp handle_election_timeout(:pre_vote, state0) do
    {context, election_effects} =
      RaftEx.Server.Election.start_election(:pre_vote, state0.server_state)

    state = %{state0 | election_context: context}
    {state, election_effects ++ set_election_timeout(state, [])}
  end

  defp handle_election_timeout(:candidate, state0) do
    {context, election_effects} =
      RaftEx.Server.Election.start_election(:candidate, state0.server_state)

    state = %{state0 | election_context: context}
    {state, election_effects ++ set_election_timeout(state, [])}
  end

  defp set_election_timeout(%{conf: conf} = state, actions) do
    action = election_timeout_action(:short, conf)
    {%{state | election_timeout_set: true}, [action | actions]}
  end

  defp handle_effects(raft_state, effects, evt_type, state, actions \\ []) do
    Enum.reduce(effects, {state, actions}, fn
      effs, {s, a} when is_list(effs) ->
        handle_effects(raft_state, effs, evt_type, s, a)

      eff, {s, a} ->
        handle_effect(raft_state, eff, evt_type, s, a)
    end)
    |> then(fn {s, a} -> {s, Enum.reverse(a)} end)
  end

  defp handle_effect(_, {:next_event, evt}, evt_type, state, actions) do
    {state, [{:next_event, evt_type, evt} | actions]}
  end

  defp handle_effect(_, {:next_event, _, _} = next, _, state, actions) do
    {state, [next | actions]}
  end

  defp handle_effect(:leader, {:reply, from, reply}, _, state, actions) do
    :gen_statem.reply(from, reply)
    {state, actions}
  end

  defp handle_effect(_, {:reply, reply}, {:call, from}, state, actions) do
    :gen_statem.reply(from, reply)
    {state, actions}
  end

  defp handle_effect(_, {:aux, cmd}, evt_type, %{server_state: ss0} = state0, actions0) do
    {_, ss, effects} = RaftEx.Server.handle_aux(:follower, :cast, cmd, ss0)
    {state, actions} = handle_effects(:follower, effects, evt_type, %{state0 | server_state: ss})
    {state, actions0 ++ actions}
  end

  defp handle_effect(_, :garbage_collection, _, state, actions) do
    :erlang.garbage_collect()
    {state, actions}
  end

  defp handle_effect(
         :leader,
         {:release_cursor, idx, mac_state},
         evt_type,
         %{server_state: ss0} = state0,
         actions0
       ) do
    {ss, effects} = RaftEx.Server.update_release_cursor(idx, mac_state, %{}, ss0)
    handle_effects(:leader, effects, evt_type, %{state0 | server_state: ss}, actions0)
  end

  defp handle_effect(
         _,
         {:bg_work, fun_or_mfa, err_fun},
         _,
         %{conf: %{worker_pid: wp}} = state,
         actions
       ) do
    if wp, do: RaftEx.Worker.queue_work(wp, fun_or_mfa, err_fun)
    {state, actions}
  end

  defp handle_effect(
         :leader,
         {:send_snapshot, peer_id, {snap_state, leader_id, term}},
         _evt_type,
         %{conf: conf} = state0,
         actions
       ) do
    snapshot_data = %{
      snap_state: snap_state,
      leader_id: leader_id,
      term: term,
      peer_id: peer_id,
      chunk_size: Map.get(conf, :snapshot_chunk_size, 65_536),
      offset: 0
    }

    {state, rpc} = build_and_send_snapshot_chunk(snapshot_data, state0)
    {state, [rpc | actions]}
  end

  defp handle_effect(
         _s,
         {:send_rpc, peer_id, rpc},
         _evt_type,
         %{election_context: %{state: election_state}} = state,
         actions
       )
       when election_state in [:pre_vote, :candidate] do
    # Track peer for election reply correlation
    send_rpc_with_tracking(peer_id, rpc, state)
    {state, actions}
  end

  defp handle_effect(_, {:send_rpc, to, rpc}, _, state, actions) do
    send_rpc(to, rpc, state)
    {state, actions}
  end

  defp handle_effect(_, _, _, state, actions), do: {state, actions}

  defp send_rpc({name, node} = to, msg, state) do
    incr_counter(state.conf, :rpcs_sent, 1)

    if node == node() do
      case Process.whereis(name) do
        nil -> :ok
        pid -> :gen_statem.cast(pid, {self(), {:rpc, msg}})
      end
    else
      RaftEx.Network.send_rpc(to, msg)
    end
  end

  defp send_rpc_with_tracking(peer_id, rpc, state) do
    incr_counter(state.conf, :rpcs_sent, 1)
    {name, node} = peer_id

    if node == node() do
      case Process.whereis(name) do
        nil ->
          :ok

        pid ->
          # Track this peer for reply correlation
          Process.put({:election_peer, pid}, peer_id)
          :gen_statem.cast(pid, {self(), {:rpc, rpc}})
      end
    else
      RaftEx.Network.send_rpc(peer_id, rpc)
    end
  end

  defp build_and_send_snapshot_chunk(
         %{
           snap_state: snap_state,
           leader_id: leader_id,
           term: term,
           peer_id: peer_id,
           chunk_size: chunk_size,
           offset: offset
         } = snap_data,
         state
       ) do
    # Serialize snapshot state into binary
    data = :erlang.term_to_binary(snap_state)
    total_size = byte_size(data)

    # Calculate chunk boundaries
    chunk_end = min(offset + chunk_size, total_size)
    chunk_data = binary_part(data, offset, chunk_end - offset)
    is_last = chunk_end >= total_size

    # Build InstallSnapshot RPC
    rpc = %RaftEx.Types.InstallSnapshotRpc{
      term: term,
      leader_id: leader_id,
      meta: %{
        last_index: snap_state.last_index || 0,
        last_term: snap_state.last_term || 0,
        total_size: total_size,
        offset: offset,
        is_last: is_last
      },
      chunk_state: if(is_last, do: :last, else: :more),
      data: chunk_data
    }

    # Update snapshot data for next chunk
    new_snap_data = %{snap_data | offset: chunk_end}

    # Store snapshot transfer state in process dictionary for reply handling
    Process.put({:snapshot_send, peer_id}, new_snap_data)

    # Update peer status to sending_snapshot
    ss =
      RaftEx.Server.update_peer(
        peer_id,
        %{status: {:sending_snapshot, self(), offset}},
        state.server_state
      )

    state = %{state | server_state: ss}

    {state, {:send_rpc, peer_id, rpc}}
  end

  defp election_timeout_action(:short, %{broadcast_time: bt}) do
    t = :rand.uniform(bt * @default_election_mult) + bt
    {:state_timeout, t, :election_timeout}
  end

  defp next_state(:leader, %{pending_commands: pending} = state, actions) do
    next_events = Enum.map(pending, fn {f, cmd} -> {:next_event, {:call, f}, cmd} end)

    {:next_state, :leader, %{state | election_timeout_set: false, pending_commands: []},
     actions ++ next_events}
  end

  defp next_state(next, state, actions) do
    {:next_state, next, %{state | election_timeout_set: false}, actions}
  end

  defp make_rpcs(%{server_state: ss0} = state) do
    {ss, rpcs} = RaftEx.Server.make_rpcs(ss0)
    {%{state | server_state: ss}, rpcs}
  end

  defp do_state_query(spec, %{server_state: ss}), do: RaftEx.Server.state_query(spec, ss)
  defp id(%{server_state: ss}), do: RaftEx.Server.id(ss)
  defp record_cluster_change(_state), do: :ok
  defp incr_counter(_, _, _), do: :ok
  defp unwrap_rpc({:rpc, msg}), do: unwrap_rpc(msg)
  defp unwrap_rpc({:gen_cast, msg}), do: unwrap_rpc(msg)
  defp unwrap_rpc(msg), do: msg

  defp unwrap_from({:rpc, msg}), do: unwrap_from(msg)
  defp unwrap_from({:gen_cast, msg}), do: unwrap_from(msg)
  defp unwrap_from({from, msg}) when is_pid(from), do: {from, unwrap_rpc(msg)}
  defp unwrap_from(msg), do: {nil, unwrap_rpc(msg)}

  defp pid_to_server_id(pid) do
    case Process.info(pid, :registered_name) do
      [{:registered_name, name}] -> {name, node()}
      _ -> nil
    end
  end

  defp set_tick_timer(%{conf: %{tick_timeout: tick_timeout}}, actions) do
    [{{:timeout, :tick}, tick_timeout, :tick_timeout} | actions]
  end
end
