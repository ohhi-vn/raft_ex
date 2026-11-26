defmodule RaftEx.App.ServerRuntime do
  @moduledoc """
  Gen_statem process managing a RaftEx server lifecycle.
  This is a partial implementation showing the structure.
  Full implementation would mirror ra_server_proc.erl closely.
  """

  @behaviour :gen_statem

  require Logger

  alias RaftEx.Domain.LeakyIntegrator
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Election
  alias RaftEx.Domain.Utils
  alias RaftEx.Infra.Network.Transport
  alias RaftEx.Infra.Observability.Metrics
  alias RaftEx.Infra.Observability.Monitor
  alias RaftEx.Infra.Persistence.LogMeta
  alias RaftEx.Infra.Persistence.LogSnapshot
  alias RaftEx.Infra.Process.Worker
  alias RaftEx.Infra.Registry.Directory

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
    name = Utils.ra_server_id_to_local_name(id)
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
    ss = Server.recover(ss0)
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
    {state, actions0} = handle_enter(:leader, old_state, state0)
    :ok = record_cluster_change(state)

    # Append a no-op entry in the new term so prior-term entries can commit
    # and cluster changes become permitted only after it commits.
    ss0 = state.server_state
    mac_ver = ss0.cfg.machine_version

    {_next, ss1, noop_effects} =
      Server.handle_leader({:command, {:noop, %{}, mac_ver}}, ss0)

    state = %{state | server_state: ss1, election_timeout_set: false}

    {state, actions1} = handle_effects(:leader, noop_effects, :cast, state, actions0)

    cluster_name = state.conf[:cluster_name]
    leader_id = Server.id(state.server_state)
    members = Map.keys(state.server_state.cluster)

    if cluster_name do
      RaftEx.Leaderboard.record(cluster_name, leader_id, members)
    end

    {:keep_state, state, actions1}
  end

  def leader({:call, from}, :trigger_election, state) do
    {:keep_state, state, [{:reply, from, {:error, :already_leader}}]}
  end

  def leader(:info, {:send_snapshot_chunk, peer_id}, state0) do
    case Map.get(state0, :snapshot_sends, %{}) |> Map.fetch(peer_id) do
      {:ok, send_state} ->
        {state, actions} = send_next_snapshot_chunk(peer_id, send_state, state0, [])
        {:keep_state, state, actions}

      :error ->
        {:keep_state, state0, []}
    end
  end

  def leader(:state_timeout, :clear_transfer_leadership, state0) do
    state = %{state0 | server_state: Map.put(state0.server_state, :transfer_leadership, nil)}
    {:keep_state, state, []}
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
    effects = Server.tick(state1.server_state)
    ss = Server.log_tick(state1.server_state)

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
      when is_tuple(cmd) and
             elem(cmd, 0) in [:"$ra_join", :"$ra_leave", :"$ra_promote", :"$ra_demote"] do
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
        do: put_in(state0.server_state.rpc_from, pid_to_server_id(from_pid)),
        else: state0

    state =
      case evt_type do
        {:call, from} -> %{state | server_state: Map.put(state.server_state, :query_from, from)}
        _ -> state
      end

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
      if Server.new?(state.server_state) do
        actions0
      else
        {_state2, actions2} = set_election_timeout(state, actions0)
        actions2
      end

    leader_id = Server.leader_id(state.server_state)
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
    state = %{state0 | server_state: Server.clear_leader_id(ss0)}
    next_state(:candidate, state, [{:reply, from, :ok}])
  end

  def follower({:call, from}, {:leader_call, _msg}, state0) do
    leader_id = Server.leader_id(state0.server_state)

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
        do: put_in(state0.server_state.rpc_from, pid_to_server_id(from_pid)),
        else: state0

    state =
      case evt_type do
        {:call, from} -> %{state | server_state: Map.put(state.server_state, :query_from, from)}
        _ -> state
      end

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
      Election.start_election(:pre_vote, state.server_state)

    state = %{state | election_context: context}
    {s, actions} = handle_effects(:pre_vote, election_effects, :cast, state, actions0)
    {s2, timeouts} = set_election_timeout(s, [])
    {:keep_state, s2, actions ++ timeouts}
  end

  def pre_vote({:call, from}, :trigger_election, %{server_state: ss0} = state0) do
    state = %{state0 | server_state: Server.clear_leader_id(ss0)}
    next_state(:candidate, state, [{:reply, from, :ok}])
  end

  def pre_vote({:call, from}, :ping, state) do
    {:keep_state, state, [{:reply, from, {:pong, :pre_vote}}]}
  end

  def pre_vote(:state_timeout, :election_timeout, state0) do
    Logger.info("RaftEx.App.ServerRuntime: Pre-vote election timeout, restarting election")
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
        do: put_in(state0.server_state.rpc_from, pid_to_server_id(from_pid)),
        else: state0

    if is_struct(inner_msg, RaftEx.Domain.Types.PreVoteResult) do
      handle_pre_vote_reply(inner_msg, {from_pid, nil}, state)
    else
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
  end

  # State: candidate
  def candidate(:enter, old_state, state0) do
    {state, actions} = handle_enter(:candidate, old_state, state0)

    {context, election_effects} =
      Election.start_election(:candidate, state.server_state)

    ss = state.server_state
    ss = %{ss | current_term: context.term, voted_for: Server.id(ss)}
    state = %{state | server_state: ss}

    Metrics.inc(:term_changed)

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
    state = %{state0 | server_state: Server.clear_leader_id(ss0)}
    next_state(:candidate, state, [{:reply, from, :ok}])
  end

  def candidate({:call, from}, {:leader_call, _msg}, state) do
    {:keep_state, state, [{:reply, from, {:error, :no_leader}}]}
  end

  def candidate({:call, from}, :ping, state) do
    {:keep_state, state, [{:reply, from, {:pong, :candidate}}]}
  end

  def candidate(:state_timeout, :election_timeout, state0) do
    Logger.info("RaftEx.App.ServerRuntime: Candidate election timeout, restarting election")
    {state, actions} = handle_election_timeout(:candidate, state0)
    next_state(:candidate, state, actions)
  end

  def candidate({:cast, {from, reply}}, :vote_reply, state0) do
    handle_vote_reply(reply, from, state0)
  end

  def candidate(:internal, :election_won, state0) do
    Logger.info("RaftEx.App.ServerRuntime: Transitioning to leader after winning election")
    {:next_state, :leader, state0, []}
  end

  def candidate(:state_timeout, :become_leader, state0) do
    Logger.info("RaftEx.App.ServerRuntime: Becoming leader after winning election")
    leader_id = Server.id(state0.server_state)
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
        do: put_in(state0.server_state.rpc_from, pid_to_server_id(from_pid)),
        else: state0

    if is_struct(inner_msg, RaftEx.Domain.Types.RequestVoteResult) do
      handle_vote_reply(inner_msg, {from_pid, nil}, state)
    else
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

  @doc false
  def statem_call_local(name, msg, timeout) when is_atom(name),
    do: statem_call(name, msg, timeout)

  defp safe_call({name, node}, msg, timeout) when node != node() do
    case :erpc.call(node, __MODULE__, :statem_call_local, [name, msg, timeout]) do
      {:badrpc, reason} -> {:error, reason}
      reply -> reply
    end
  catch
    :exit, {:timeout, _} -> :timeout
    :exit, {:noproc, _} -> {:error, :noproc}
    :exit, {{:nodedown, _}, _} -> {:error, :nodedown}
    :exit, {:shutdown, _} -> {:error, :shutdown}
    :exit, {reason, _} -> {:error, reason}
  end

  defp safe_call(server_id, msg, timeout) do
    :gen_statem.call(server_id, msg, timeout)
  catch
    :exit, {:timeout, _} -> :timeout
    :exit, {:noproc, _} -> {:error, :noproc}
    :exit, {{:nodedown, _}, _} -> {:error, :nodedown}
    :exit, {:shutdown, _} -> {:error, :shutdown}
    :exit, {reason, _} -> {:error, reason}
  end

  defp do_init(%{id: id, cluster_name: cluster_name} = config) do
    key = Utils.ra_server_id_to_local_name(id)
    true = :ets.insert(:ra_state, {key, :init, :unknown})
    Process.flag(:trap_exit, true)

    server_state = Server.init(config)

    if system = config[:system] do
      uid = config[:uid]

      Directory.register_name(
        system,
        uid,
        self(),
        self(),
        key,
        cluster_name
      )
    end

    sys_cfg = Server.system_config(server_state)
    broadcast_time = Map.get(config, :broadcast_time, Map.get(sys_cfg, :broadcast_time, 100))

    %__MODULE__{
      conf: %{
        log_id: Server.log_id(server_state),
        cluster_name: cluster_name,
        name: key,
        broadcast_time: broadcast_time,
        election_timeout_min:
          max(
            Map.get(config, :election_timeout_min, Map.get(sys_cfg, :election_timeout_min, 500)),
            Map.get(config, :tick_timeout, @tick_interval_ms) + 200
          ),
        election_timeout_max:
          Map.get(config, :election_timeout_max, Map.get(sys_cfg, :election_timeout_max, 2_000)),
        tick_timeout: Map.get(config, :tick_timeout, @tick_interval_ms),
        await_condition_timeout:
          Map.get(config, :await_condition_timeout, @default_await_condition_timeout),
        install_snap_rpc_timeout:
          Map.get(config, :install_snap_rpc_timeout, @install_snap_rpc_timeout),
        counter: Map.get(config, :counter)
      },
      server_state: server_state,
      monitors: Monitor.init(),
      pending_commands: [],
      leader_monitor: nil,
      election_timeout_set: false,
      pending_notifys: %{},
      pending_queries: [],
      commit_rate: {LeakyIntegrator.new(@tick_interval_ms * 6), 0, 0.0}
    }
  end

  defp handle_enter(raft_state, _old_state, %{conf: %{name: name}, server_state: ss0} = state) do
    membership = Server.get_membership(ss0)
    true = :ets.insert(:ra_state, {name, raft_state, membership})
    {ss, effects} = Server.handle_state_enter(raft_state, ss0)
    handle_effects(raft_state, effects, :cast, %{state | server_state: ss})
  end

  defp handle_leader(msg, %{server_state: ss0} = state0) do
    {next_state, ss, effects} = Server.handle_leader(msg, ss0)
    state = %{state0 | server_state: Server.persist_last_applied(ss)}
    {next_state, state, effects}
  catch
    :throw, {next_state, ss, effects} when is_atom(next_state) ->
      state = %{state0 | server_state: Server.persist_last_applied(ss)}
      {next_state, state, effects}
  end

  defp handle_follower(msg, %{server_state: ss0} = state0) do
    {next_state, ss, effects} = Server.handle_follower(msg, ss0)
    state = %{state0 | server_state: Server.persist_last_applied(ss)}
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
      Election.handle_pre_vote_reply(reply, ctx0, ss0, peer_id)

    state = %{state0 | election_context: ctx}

    {state1, actions} = handle_effects(:pre_vote, effects, :cast, state)

    if should_start_election do
      Logger.info("RaftEx.App.ServerRuntime: Pre-vote succeeded, transitioning to candidate")
      next_state(:candidate, state1, actions)
    else
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

    {result, ctx, effects} =
      Election.handle_vote_reply(reply, ctx0, ss0, peer_id)

    state = %{state0 | election_context: ctx}

    case result do
      :won ->
        Metrics.inc(:elections_won)
        Logger.info("RaftEx.App.ServerRuntime: Won election, becoming leader")

        {state1, actions} =
          handle_effects(:candidate, effects ++ [{:become, :leader}], :cast, state)

        next_state(:leader, state1, actions)

      :lost ->
        Metrics.inc(:elections_lost)
        Logger.info("RaftEx.App.ServerRuntime: Lost election, stepping down to follower")
        {state1, actions} = handle_effects(:candidate, effects, :cast, state)
        next_state(:follower, state1, actions)

      :ongoing ->
        {state1, actions} = handle_effects(:candidate, effects, :cast, state)
        {:keep_state, state1, actions}
    end
  end

  defp handle_election_timeout(:pre_vote, state0) do
    {context, election_effects} =
      Election.start_election(:pre_vote, state0.server_state)

    state = %{state0 | election_context: context}
    {state, actions} = handle_effects(:pre_vote, election_effects, :cast, state)
    set_election_timeout(state, actions)
  end

  defp handle_election_timeout(:candidate, state0) do
    {context, election_effects} =
      Election.start_election(:candidate, state0.server_state)

    ss = state0.server_state
    ss = %{ss | current_term: context.term, voted_for: Server.id(ss)}
    state = %{state0 | server_state: ss, election_context: context}

    Metrics.inc(:term_changed)

    {state, actions} = handle_effects(:candidate, election_effects, :cast, state)
    set_election_timeout(state, actions)
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
    {_, ss, effects} = Server.handle_aux(:follower, :cast, cmd, ss0)
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
    {ss, effects} = Server.update_release_cursor(idx, mac_state, %{}, ss0)
    handle_effects(:leader, effects, evt_type, %{state0 | server_state: ss}, actions0)
  end

  defp handle_effect(
         _,
         {:bg_work, fun_or_mfa, err_fun},
         _,
         %{conf: %{worker_pid: wp}} = state,
         actions
       ) do
    if wp, do: Worker.queue_work(wp, fun_or_mfa, err_fun)
    {state, actions}
  end

  defp handle_effect(
         :leader,
         {:send_snapshot, peer_id, {snap_state, leader_id, term}},
         _evt_type,
         state0,
         actions
       ) do
    sends = Map.get(state0, :snapshot_sends, %{})

    case Map.fetch(sends, peer_id) do
      {:ok, send_state} ->
        send_next_snapshot_chunk(peer_id, send_state, state0, actions)

      :error ->
        snap_dir = snapshot_dir(state0)

        case LogSnapshot.begin_read(snap_dir, %{
               can_accept_full_file: false
             }) do
          {:ok, meta, read_state} ->
            send_state = %{
              meta: meta,
              read_state: read_state,
              offset: 0,
              snap_state: snap_state,
              leader_id: leader_id,
              term: term
            }

            state = Map.put(state0, :snapshot_sends, Map.put(sends, peer_id, send_state))
            send_next_snapshot_chunk(peer_id, send_state, state, actions)

          _err ->
            {state0, actions}
        end
    end
  end

  defp handle_effect(:leader, {:clear_snapshot_send, peer_id}, _, state0, actions) do
    sends = Map.delete(Map.get(state0, :snapshot_sends, %{}), peer_id)
    {Map.put(state0, :snapshot_sends, sends), actions}
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

  defp handle_effect(
         raft_state,
         {:apply_committed, idx},
         evt_type,
         %{server_state: ss0} = state,
         actions
       ) do
    {ss, effects} = Server.apply_committed(idx, ss0)

    # Checkpoint the applied index immediately: a client may observe the
    # state machine change in the same tick the server is stopped, leaving no
    # subsequent message to trigger the lazy persistence in handle_leader.
    ss = Server.persist_last_applied(ss)

    handle_effects(raft_state, effects, evt_type, %{state | server_state: ss}, actions)
  end

  defp handle_effect(_, {:broadcast_commit, idx}, _, %{server_state: ss} = state, actions) do
    leader = ss.cfg.id

    for peer <- Map.keys(ss.cluster) -- [leader] do
      send_rpc(
        peer,
        %RaftEx.Domain.Types.HeartbeatRpc{
          leader_id: leader,
          term: ss.current_term,
          query_index: 0,
          commit_index: idx
        },
        state
      )
    end

    {state, actions}
  end

  defp handle_effect(_, {:store_meta, key, value}, _, %{server_state: ss} = state, actions) do
    meta_name = Server.system_config(ss) |> meta_name_from_sc()
    uid = Server.uid(ss)

    try do
      LogMeta.store_sync(meta_name, uid, key, value)
    catch
      _, _ -> :ok
    end

    {state, actions}
  end

  defp handle_effect(_, {:reset_election_timeout}, _, state, actions) do
    set_election_timeout(state, actions)
  end

  defp handle_effect(_, {:transfer_leadership_timeout, _target}, _, state, actions) do
    {state, [{:state_timeout, 1_000, :clear_transfer_leadership} | actions]}
  end

  defp handle_effect(_, _, _, state, actions), do: {state, actions}

  defp send_next_snapshot_chunk(peer_id, send_state, state0, actions) do
    chunk_size = Map.get(state0.conf, :snapshot_chunk_size, 65_536)
    snap_dir = snapshot_dir(state0)

    case LogSnapshot.read_chunk(
           send_state.read_state,
           chunk_size,
           snap_dir
         ) do
      {:ok, data, :last} ->
        Metrics.inc(:snapshot_chunks_sent)
        chunk_state = if send_state.offset == 0, do: :full, else: :last
        payload = if chunk_state == :full, do: send_state.snap_state, else: data

        rpc = %RaftEx.Domain.Types.InstallSnapshotRpc{
          term: send_state.term,
          leader_id: send_state.leader_id,
          meta: send_state.meta,
          chunk_state: chunk_state,
          data: payload
        }

        send_snapshot_rpc(peer_id, rpc, state0)

        sends = Map.delete(Map.get(state0, :snapshot_sends, %{}), peer_id)
        {Map.put(state0, :snapshot_sends, sends), [rpc | actions]}

      {:ok, data, {:next, new_read_state}} ->
        Metrics.inc(:snapshot_chunks_sent)
        chunk_state = if send_state.offset == 0, do: :first, else: :middle

        rpc = %RaftEx.Domain.Types.InstallSnapshotRpc{
          term: send_state.term,
          leader_id: send_state.leader_id,
          meta: send_state.meta,
          chunk_state: chunk_state,
          data: data
        }

        send_snapshot_rpc(peer_id, rpc, state0)

        new_read_state =
          case send_state.read_state do
            {crc, _} when is_integer(crc) -> {crc, new_read_state}
            _ -> new_read_state
          end

        new_send_state = %{
          send_state
          | read_state: new_read_state,
            offset: send_state.offset + chunk_size
        }

        sends = Map.put(Map.get(state0, :snapshot_sends, %{}), peer_id, new_send_state)
        state = Map.put(state0, :snapshot_sends, sends)
        {state, [rpc, {:next_event, :info, {:send_snapshot_chunk, peer_id}} | actions]}

      {:error, _reason} ->
        sends = Map.delete(Map.get(state0, :snapshot_sends, %{}), peer_id)
        {Map.put(state0, :snapshot_sends, sends), actions}
    end
  end

  defp send_snapshot_rpc({name, node} = _peer_id, rpc, state)
       when node == node() do
    _ = state

    case Process.whereis(name) do
      nil -> :ok
      pid -> :gen_statem.cast(pid, {self(), {:rpc, rpc}})
    end
  end

  defp send_snapshot_rpc(peer_id, rpc, _state) do
    Transport.send_rpc(peer_id, rpc)
  end

  defp snapshot_dir(%{server_state: %{log: %{data_dir: dir}}}) when is_binary(dir) do
    Path.join(dir, "snapshot")
  end

  defp snapshot_dir(_state) do
    Path.join(System.tmp_dir!(), "raft_ex_snapshot")
  end

  defp meta_name_from_sc(%{names: %{log_meta: name}}), do: name
  defp meta_name_from_sc(sc), do: Server.meta_name(sc)

  defp send_rpc({name, node} = to, msg, state) do
    incr_counter(state.conf, :rpcs_sent, 1)

    if node == node() do
      case Process.whereis(name) do
        nil -> :ok
        pid -> :gen_statem.cast(pid, {self(), {:rpc, msg}})
      end
    else
      Transport.send_rpc(to, msg)
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
      Transport.send_rpc(peer_id, rpc)
    end
  end

  defp election_timeout_action(:short, conf) do
    bt = Map.get(conf, :broadcast_time, 100)
    min_t = Map.get(conf, :election_timeout_min, bt)
    max_t = Map.get(conf, :election_timeout_max, bt * @default_election_mult)

    lo = min(min_t, max_t)
    hi = max(min_t, max_t)
    t = lo + :rand.uniform(max(hi - lo, 1))
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
    {ss, rpcs} = Server.make_rpcs(ss0)
    {%{state | server_state: ss}, rpcs}
  end

  defp do_state_query(spec, %{server_state: ss}), do: Server.state_query(spec, ss)
  defp id(%{server_state: ss}), do: Server.id(ss)
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
    pid_node = node(pid)

    info =
      if pid_node == node() do
        Process.info(pid, :registered_name)
      else
        try do
          :erpc.call(pid_node, Process, :info, [pid, :registered_name], 2_000)
        catch
          _, _ -> nil
        end
      end

    case info do
      {:registered_name, name} when is_atom(name) -> {name, pid_node}
      [{:registered_name, name}] when is_atom(name) -> {name, pid_node}
      _ -> nil
    end
  end

  defp set_tick_timer(%{conf: %{tick_timeout: tick_timeout}}, actions) do
    [{{:timeout, :tick}, tick_timeout, :tick_timeout} | actions]
  end
end
