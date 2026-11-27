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
  alias RaftEx.Domain.Server.Cluster
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
    :election_context,
    cmd_batch_queue: :queue.new(),
    batch_flush_scheduled?: false
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

    # Send an immediate heartbeat/AppendEntries round so peers learn about the
    # new leader before their election timeouts fire; otherwise a 2-voter (or
    # slow) cluster ping-pongs leadership indefinitely.
    {state2, rpc_effs} = make_rpcs(state)
    effects = Server.tick(state2.server_state)
    ss2 = Server.log_tick(state2.server_state)

    {state3, actions2} =
      handle_effects(:leader, rpc_effs ++ effects ++ [{:aux, :tick}], :cast, %{
        state2
        | server_state: ss2
      })

    {:keep_state, state3, set_tick_timer(state3, drop_next_events(actions1 ++ actions2))}
  end

  def leader({:call, from}, :trigger_election, state) do
    {:keep_state, state, [{:reply, from, {:error, :already_leader}}]}
  end

  def leader(:info, :pipeline_rpcs, state0) do
    {state1, rpc_effs} = make_pipeline_rpcs(state0)

    if rpc_effs == [] do
      {:keep_state, state1, []}
    else
      {state2, actions} = handle_effects(:leader, rpc_effs, :cast, state1)
      {:keep_state, state2, actions}
    end
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

  def leader(:state_timeout, {:snapshot_send_timeout, peer_id}, state0) do
    case Map.fetch(Map.get(state0, :snapshot_sends, %{}), peer_id) do
      :error ->
        # Transfer already completed or abandoned; nothing to do.
        {:keep_state, state0, []}

      {:ok, _send_state} ->
        Logger.warning(
          "RaftEx.App.ServerRuntime: snapshot transfer to #{inspect(peer_id)} timed out, abandoning and retrying later"
        )

        sends = Map.delete(Map.get(state0, :snapshot_sends, %{}), peer_id)
        state = Map.put(state0, :snapshot_sends, sends)
        state = update_peer_status(state, peer_id, :normal)
        {:keep_state, state, [{:next_event, :info, :pipeline_rpcs}]}
    end
  end

  def leader({:call, from}, :ping, state) do
    {:keep_state, state, [{:reply, from, {:pong, :leader}}]}
  end

  # Local state reads bypass leader redirection: the caller explicitly asked
  # *this* server, in whatever state it is in.
  def leader({:call, from}, {:local_call, {:state_query, spec}}, state) do
    {:keep_state, state, [{:reply, from, {:ok, do_state_query(spec, state), id(state)}}]}
  end

  def leader({:call, from}, {:local_call, _msg}, state) do
    {:keep_state, state, [{:reply, from, {:error, :unsupported}}]}
  end

  def leader({:call, from}, {:state_query, spec}, state) do
    reply = {:ok, do_state_query(spec, state), id(state)}
    {:keep_state, state, [{:reply, from, reply}]}
  end

  def leader({:timeout, :tick}, :tick_timeout, state) do
    {state1, rpc_effs} = make_rpcs(reap_suspects(state))
    effects = Server.tick(state1.server_state)
    ss = Server.log_tick(state1.server_state)

    {state2, tick_actions} =
      handle_effects(:leader, rpc_effs ++ effects ++ [{:aux, :tick}], :cast, %{
        state1
        | server_state: ss
      })

    timeout = Map.get(state2.conf, :election_timeout_max, 2_000)
    now_ms = System.monotonic_time(:millisecond)

    # Time-based automatic compaction check for otherwise-idle leaders; the
    # entry and WAL-byte thresholds are checked on every apply batch (14.1).
    {ss2, auto_effects} = Server.maybe_auto_compact(state2.server_state, now_ms)

    {state2, actions} =
      handle_effects(:leader, auto_effects, :cast, %{state2 | server_state: ss2})

    case Server.check_quorum(state2.server_state, now_ms, timeout) do
      {:leader, new_ss, _} ->
        {:keep_state, %{state2 | server_state: new_ss},
         set_tick_timer(state2, tick_actions ++ actions)}

      {:follower, new_ss, _} ->
        Logger.warning("RaftEx.ServerRuntime: lost quorum, stepping down to follower")
        next_state(:follower, %{state2 | server_state: new_ss}, tick_actions ++ actions)
    end
  end

  def leader({:call, from}, {:leader_call, {:command, cmd_usr}}, state0)
      when is_tuple(cmd_usr) and elem(cmd_usr, 0) == :"$usr" do
    {:"$usr", data, reply_mode} = cmd_usr
    cmd = {:"$usr", %{from: from}, data, reply_mode}
    leader({:call, from}, {:command, cmd}, state0)
  end

  def leader({:call, from}, {:leader_call, {:command, cmd}}, state0)
      when is_tuple(cmd) and
             elem(cmd, 0) in [
               :"$ra_join",
               :"$ra_leave",
               :"$ra_promote",
               :"$ra_demote",
               :"$ra_cluster"
             ] do
    {tag, data, reply_mode} = cmd
    cmd = {tag, %{from: from}, data, reply_mode}
    leader({:call, from}, {:command, cmd}, state0)
  end

  def leader({:call, from}, {:leader_call, msg}, state0) do
    leader({:call, from}, msg, state0)
  end

  # Batched command flush: drain the pending user-command queue through a
  # single `{:commands, cmds}` domain call (one WAL write for the batch).
  def leader(:info, :flush_commands, state0) do
    q = Map.get(state0, :cmd_batch_queue, :queue.new())

    state1 =
      state0
      |> Map.put(:cmd_batch_queue, :queue.new())
      |> Map.put(:batch_flush_scheduled?, false)

    case :queue.to_list(q) do
      [] ->
        {:keep_state, state1, []}

      cmds ->
        leader(:info, {:commands, cmds}, state1)
    end
  end

  def leader(:info, {:ra_network_event, event, node}, state) do
    handle_network_event(:leader, event, node, state)
  end

  def leader(evt_type, msg, state0) do
    {from_sender, inner_msg} = unwrap_from(msg)

    state =
      case resolve_sender(from_sender, inner_msg) do
        nil -> state0
        sender_id -> %{state0 | server_state: Map.put(state0.server_state, :rpc_from, sender_id)}
      end

    state =
      case evt_type do
        {:call, from} -> %{state | server_state: Map.put(state.server_state, :query_from, from)}
        _ -> state
      end

    case inner_msg do
      {:command, {:"$usr", _, _, _} = cmd}
      when is_tuple(evt_type) and elem(evt_type, 0) == :call ->
        {:call, from} = evt_type
        enqueue_command(from, cmd, state)

      {:command, _} = command_msg
      when is_tuple(evt_type) and elem(evt_type, 0) == :call ->
        {:call, from} = evt_type
        q = Map.get(state, :cmd_batch_queue, :queue.new())

        if :queue.is_empty(q) do
          route_leader_command(inner_msg, evt_type, state)
        else
          # A batch is already queued ahead; preserve submission order.
          {tag, data, mode} =
            case command_msg do
              {:command, {t, d, m}} -> {t, d, m}
              {:command, {t, _, d, m}} -> {t, d, m}
            end

          enqueue_command(from, {tag, %{from: from}, data, mode}, state)
        end

      _ ->
        route_leader_command(inner_msg, evt_type, state)
    end
  end

  defp route_leader_command(inner_msg, evt_type, state) do
    case handle_leader(inner_msg, state) do
      {:leader, state1, effects} ->
        {state, actions} = handle_effects(:leader, effects, evt_type, state1)
        {:keep_state, state, actions}

      {:stop, state1, effects} ->
        {state, _} = handle_effects(:leader, effects, evt_type, state1)
        drain_batch_queue(state, :shutdown)

        stopped =
          Map.merge(state, %{cmd_batch_queue: :queue.new(), batch_flush_scheduled?: false})

        {:stop, :normal, stopped}

      {:await_condition, state1, effects} ->
        {state, actions} = handle_effects(:leader, effects, evt_type, state1)
        next_state(:await_condition, state, actions)

      {next_state, state1, effects} ->
        {state, actions} = handle_effects(:leader, effects, evt_type, state1)
        next_state(next_state, state, actions)
    end
    |> ensure_call_replied(evt_type, inner_msg)
  end

  # Bounded pending-command queue (task 8.1). User commands are collected
  # and flushed as one batch (single WAL write); when the queue is full the
  # new command is rejected immediately with a backpressure error.
  defp enqueue_command(from, cmd, state) do
    q = Map.get(state, :cmd_batch_queue, :queue.new())
    max = Map.get(state.conf, :max_pending_commands, 1024)

    if :queue.len(q) >= max do
      # Direct reply (no marker): this path never goes through
      # `handle_effects`, so there is nothing for `ensure_call_replied`
      # to strip, and the caller already has its exactly-one reply.
      :gen_statem.reply(from, {:error, :backpressure})
      {:keep_state, state, []}
    else
      state = Map.put(state, :cmd_batch_queue, :queue.in(cmd, q))
      {:keep_state, schedule_batch_flush(state), []}
    end
  end

  defp schedule_batch_flush(state) do
    if Map.get(state, :batch_flush_scheduled?, false) do
      state
    else
      linger = Map.get(state.conf, :command_batch_linger_ms, 0)
      Process.send_after(self(), :flush_commands, linger)
      Map.put(state, :batch_flush_scheduled?, true)
    end
  end

  # Reply to every queued caller with an error (used when the server stops
  # being leader with unflushed commands). Direct replies: the queue only
  # exists on call paths that `ensure_call_replied/3` already passed, and
  # `:stop` results cannot carry actions.
  defp drain_batch_queue(state, reason) do
    q = Map.get(state, :cmd_batch_queue, :queue.new())

    for cmd <- :queue.to_list(q) do
      case cmd do
        {_, %{from: from}, _, _} when from != nil -> :gen_statem.reply(from, {:error, reason})
        _ -> :ok
      end
    end

    :ok
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

  def follower({:call, from}, {:local_call, {:state_query, spec}}, state) do
    {:keep_state, state, [{:reply, from, {:ok, do_state_query(spec, state), id(state)}}]}
  end

  def follower({:call, from}, {:local_call, _msg}, state) do
    {:keep_state, state, [{:reply, from, {:error, :unsupported}}]}
  end

  def follower(:info, {:ra_network_event, event, node}, state) do
    handle_network_event(:follower, event, node, state)
  end

  def follower(evt_type, msg, state0) do
    {from_sender, inner_msg} = unwrap_from(msg)

    state =
      case resolve_sender(from_sender, inner_msg) do
        nil -> state0
        sender_id -> %{state0 | server_state: Map.put(state0.server_state, :rpc_from, sender_id)}
      end

    state =
      case evt_type do
        {:call, from} -> %{state | server_state: Map.put(state.server_state, :query_from, from)}
        _ -> state
      end

    case handle_follower(inner_msg, state) do
      {:stop, state1, effects} ->
        {state, _} = handle_effects(:follower, effects, evt_type, state1)
        {:stop, :normal, state}

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
    |> ensure_call_replied(evt_type, inner_msg)
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

  def pre_vote({:call, from}, {:local_call, {:state_query, spec}}, state) do
    {:keep_state, state, [{:reply, from, {:ok, do_state_query(spec, state), id(state)}}]}
  end

  def pre_vote({:call, from}, {:local_call, _msg}, state) do
    {:keep_state, state, [{:reply, from, {:error, :unsupported}}]}
  end

  def pre_vote(:state_timeout, :election_timeout, state0) do
    Logger.info("RaftEx.App.ServerRuntime: Pre-vote election timeout, restarting election")
    {state, actions} = handle_election_timeout(:pre_vote, state0)
    next_state(:pre_vote, state, actions)
  end

  def pre_vote({:cast, {from, reply}}, :pre_vote_reply, state0) do
    handle_pre_vote_reply(reply, from, state0)
  end

  def pre_vote(:info, {:ra_network_event, event, node}, state) do
    handle_network_event(:pre_vote, event, node, state)
  end

  def pre_vote(evt_type, msg, state0) do
    {from_sender, inner_msg} = unwrap_from(msg)

    state =
      case resolve_sender(from_sender, inner_msg) do
        nil -> state0
        sender_id -> %{state0 | server_state: Map.put(state0.server_state, :rpc_from, sender_id)}
      end

    if is_struct(inner_msg, RaftEx.Domain.Types.PreVoteResult) do
      handle_pre_vote_reply(inner_msg, {from_sender, nil}, state)
    else
      case handle_follower(inner_msg, state) do
        {:stop, state1, effects} ->
          {state, _} = handle_effects(:pre_vote, effects, evt_type, state1)
          {:stop, :normal, state}

        {:pre_vote, state1, effects} ->
          {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
          {:keep_state, state, actions}

        {:follower, state1, effects} ->
          # Same rule as for candidates: only genuine leader traffic, a
          # higher term, or giving our vote away ends the pre-vote round.
          if campaign_demoted?(inner_msg, state, state1) do
            {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
            next_state(:follower, state, actions)
          else
            {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
            {:keep_state, state, actions}
          end

        {:candidate, state1, effects} ->
          {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
          next_state(:candidate, state, actions)

        {next_s, state1, effects} ->
          {state, actions} = handle_effects(:pre_vote, effects, evt_type, state1)
          next_state(next_s, state, actions)
      end
      |> ensure_call_replied(evt_type, inner_msg)
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

  def candidate({:call, from}, {:local_call, {:state_query, spec}}, state) do
    {:keep_state, state, [{:reply, from, {:ok, do_state_query(spec, state), id(state)}}]}
  end

  def candidate({:call, from}, {:local_call, _msg}, state) do
    {:keep_state, state, [{:reply, from, {:error, :unsupported}}]}
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

  def candidate(:info, {:ra_network_event, event, node}, state) do
    handle_network_event(:candidate, event, node, state)
  end

  def candidate(evt_type, msg, state0) do
    {from_sender, inner_msg} = unwrap_from(msg)

    state =
      case resolve_sender(from_sender, inner_msg) do
        nil -> state0
        sender_id -> %{state0 | server_state: Map.put(state0.server_state, :rpc_from, sender_id)}
      end

    if is_struct(inner_msg, RaftEx.Domain.Types.RequestVoteResult) do
      handle_vote_reply(inner_msg, {from_sender, nil}, state)
    else
      case handle_follower(inner_msg, state) do
        {:stop, state1, effects} ->
          {state, _} = handle_effects(:candidate, effects, evt_type, state1)
          {:stop, :normal, state}

        {:candidate, state1, effects} ->
          {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
          {:keep_state, state, actions}

        {:follower, state1, effects} ->
          # A candidate is only demoted by genuine leader traffic, a higher
          # term, or giving its vote away. Polls, denied votes, local
          # queries, and maintenance events must not abort the campaign.
          if campaign_demoted?(inner_msg, state, state1) do
            {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
            next_state(:follower, state, actions)
          else
            {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
            {:keep_state, state, actions}
          end

        {:pre_vote, state1, effects} ->
          {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
          next_state(:pre_vote, state, actions)

        {next_s, state1, effects} ->
          {state, actions} = handle_effects(:candidate, effects, evt_type, state1)
          next_state(next_s, state, actions)
      end
      |> ensure_call_replied(evt_type, inner_msg)
    end
  end

  def receive_snapshot(:enter, _, state) do
    {state, actions} = reset_snapshot_timer(state, [])
    {:keep_state, state, actions}
  end

  def receive_snapshot({:call, from}, {:local_call, {:state_query, spec}}, state) do
    {:keep_state, state, [{:reply, from, {:ok, do_state_query(spec, state), id(state)}}]}
  end

  def receive_snapshot({:call, from}, {:local_call, _msg}, state) do
    {:keep_state, state, [{:reply, from, {:error, :unsupported}}]}
  end

  def receive_snapshot(:state_timeout, :snapshot_transfer_timeout, state0) do
    Logger.warning(
      "RaftEx.App.ServerRuntime: snapshot transfer timed out, discarding partial transfer"
    )

    {:next_state, :follower, abort_snapshot_transfer(state0), []}
  end

  def receive_snapshot(:info, {:ra_network_event, event, node}, state) do
    handle_network_event(:receive_snapshot, event, node, state)
  end

  def receive_snapshot(evt_type, msg, state0) do
    {from_sender, inner_msg} = unwrap_from(msg)

    state =
      case resolve_sender(from_sender, inner_msg) do
        nil -> state0
        sender_id -> %{state0 | server_state: Map.put(state0.server_state, :rpc_from, sender_id)}
      end

    case handle_follower(inner_msg, state) do
      {:stop, state1, effects} ->
        {state, _} = handle_effects(:follower, effects, evt_type, state1)
        {:stop, :normal, abort_snapshot_transfer(state)}

      {:receive_snapshot, state1, effects} ->
        {state, actions} = handle_effects(:follower, effects, evt_type, state1)
        {state, actions} = reset_snapshot_timer(state, actions)
        {:keep_state, state, actions}

      {:follower, state1, effects} ->
        {state, actions} = handle_effects(:follower, effects, evt_type, state1)
        next_state(:follower, abort_snapshot_transfer(state), actions)

      {next_s, state1, effects} ->
        {state, actions} = handle_effects(:follower, effects, evt_type, state1)
        next_state(next_s, abort_snapshot_transfer(state), actions)
    end
    |> ensure_call_replied(evt_type, inner_msg)
  end

  # Discard an in-flight partial snapshot without touching any installed one.
  defp abort_snapshot_transfer(%{server_state: ss} = state) do
    case Map.get(ss, :snapshot_state) do
      accept when is_tuple(accept) -> LogSnapshot.abort(accept)
      _ -> :ok
    end

    %{state | server_state: ss |> Map.put(:snapshot_state, nil) |> Map.put(:snapshot_meta, nil)}
  end

  # A blocked writer waits here with a deadline. Each poll re-evaluates the
  # blocking condition: recovered means transitioning back to resume work,
  # while an expired deadline steps down to follower so the server stops
  # pretending to lead without a working log. Calls arriving while blocked
  # fail fast with the blocking reason instead of hanging.
  def await_condition(:enter, _, state) do
    timeout = Map.get(state.conf, :await_condition_timeout, @default_await_condition_timeout)
    deadline = System.monotonic_time(:millisecond) + timeout

    {:keep_state, Map.put(state, :await_deadline, deadline),
     [{:state_timeout, timeout, :await_condition_timeout}]}
  end

  def await_condition(:state_timeout, :await_condition_timeout, %{server_state: ss} = state) do
    poll_await_condition(:await_condition_timeout, ss, state)
  end

  # Any other info event also re-evaluates the predicate (5.1): a blocked
  # writer retries on activity, not just on its poll timer, so no event is
  # silently ignored. A scheduled batch flush is held until the condition
  # clears (the leader re-entry path reschedules it).
  def await_condition(:info, :flush_commands, state) do
    {:keep_state, state, []}
  end

  def await_condition(:info, {:ra_network_event, event, node}, state) do
    handle_network_event(:await_condition, event, node, state)
  end

  def await_condition(:info, msg, state) do
    poll_await_condition(msg, Map.get(state, :server_state, %{}), state)
  end

  def await_condition({:call, from}, _msg, state) do
    {:keep_state, state, [{:reply, from, {:error, :wal_down}}]}
  end

  def await_condition(_evt_type, _msg, state) do
    {:keep_state, state, []}
  end

  defp poll_await_condition(msg, ss, state) do
    now = System.monotonic_time(:millisecond)
    deadline = Map.get(state, :await_deadline, now)

    case Map.get(ss, :condition, Map.get(state, :condition)) do
      %{predicate_fun: pred, transition_to: target} = _cond ->
        case apply_condition_predicate(pred, msg, ss) do
          {true, new_ss} ->
            next_state(
              target,
              Map.delete(Map.put(state, :server_state, new_ss), :await_deadline),
              []
            )

          true ->
            next_state(target, Map.delete(state, :await_deadline), [])

          _ when now >= deadline ->
            Logger.warning(
              "RaftEx.App.ServerRuntime: await_condition timed out, stepping down to follower"
            )

            ss2 = Map.delete(ss, :condition)

            next_state(
              :follower,
              Map.delete(Map.put(state, :server_state, ss2), :await_deadline),
              []
            )

          _ ->
            {:keep_state, state,
             [{:state_timeout, await_condition_poll_ms(state), :await_condition_timeout}]}
        end

      nil when msg == :await_condition_timeout ->
        # Defensive: a timeout with no condition armed can only come from a
        # stale timer; leave the blocked state rather than spinning on it.
        next_state(:follower, Map.delete(state, :await_deadline), [])

      nil ->
        {:keep_state, state, []}
    end
  end

  # Predicates take `{msg, server_state}`; single-arity predicates that only
  # need the state are also accepted.
  defp apply_condition_predicate(pred, msg, ss) do
    if is_function(pred, 2) do
      pred.(msg, ss)
    else
      pred.(ss)
    end
  end

  defp await_condition_poll_ms(%{conf: conf}) do
    min(Map.get(conf, :await_condition_timeout, @default_await_condition_timeout), 1_000)
  end

  def terminating_leader(:enter, _, state), do: {:keep_state, state, []}
  def terminating_leader(_, _, state), do: {:keep_state, state, []}

  def terminating_follower(:enter, _, state), do: {:keep_state, state, []}
  def terminating_follower(_, _, state), do: {:keep_state, state, []}

  @impl :gen_statem
  def handle_event({:call, from}, content, state_name, state) do
    # State functions handle every known message: reaching here means an
    # unsupported call that would otherwise hang the caller until its own
    # timeout. Fail fast instead.
    Logger.warning(
      "RaftEx.App.ServerRuntime: unsupported call #{inspect(content)} in state #{inspect(state_name)}"
    )

    {:next_state, state_name, state, [{:reply, from, {:error, :unsupported}}]}
  end

  @impl :gen_statem
  def handle_event(_evt_type, content, state_name, state) do
    Logger.warning(
      "RaftEx.App.ServerRuntime: unknown event #{inspect(content)} in state #{inspect(state_name)}"
    )

    {:next_state, state_name, state}
  end

  @impl :gen_statem
  def terminate(reason, _state_name, %{server_state: ss} = _state) do
    Logger.debug("ra_server_proc: terminating with #{inspect(reason)}")

    # Best-effort final apply-progress sync so the next start replays less.
    # The meta store may already be gone during system shutdown.
    try do
      Server.persist_last_applied!(ss)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end

  def terminate(_, _, _), do: :ok

  @impl :gen_statem
  def code_change(_old_vsn, state_name, state, _extra), do: {:ok, state_name, state}

  # ---- private helpers -----------------------------------------------

  defp snapshot_send_timeout(%{conf: conf}, peer_id) do
    timeout = Map.get(conf, :snapshot_send_timeout_ms, 30_000)
    {:state_timeout, timeout, {:snapshot_send_timeout, peer_id}}
  end

  defp reset_snapshot_timer(%{conf: conf} = state, actions) do
    timeout = Map.get(conf, :snapshot_transfer_timeout_ms, 30_000)
    {state, [{:state_timeout, timeout, :snapshot_transfer_timeout} | actions]}
  end

  # Guarantees every synchronous caller gets exactly one reply. Direct
  # `:gen_statem.reply/2` sends leave a `{:replied, from}` marker in the
  # accumulator (stripped before returning to `gen_statem`); a deferred
  # consistent query or aux command legitimately answers later, so those are
  # exempt. Anything else that produced no reply would hang the caller until
  # its own timeout, so fail it fast here instead.
  defp ensure_call_replied(result, {:call, from}, inner_msg) do
    case result do
      {kind, state, actions} when kind in [:keep_state, :next_state] ->
        {actions, replied?} = strip_replied(actions, from)

        cond do
          replied? -> {kind, state, actions}
          deferred_reply?(inner_msg) -> {kind, state, actions}
          has_reply_action?(actions) -> {kind, state, actions}
          true -> {kind, state, actions ++ [{:reply, from, {:error, :unsupported}}]}
        end

      _ ->
        result
    end
  end

  defp ensure_call_replied(result, _evt_type, _inner_msg) do
    # Deferred replies (commit application, notifications) happen on
    # `:info`/`:cast` events, not on the original call: the `{:replied, _}`
    # marker must never reach `gen_statem` on any path.
    case result do
      {kind, state, actions} when kind in [:keep_state, :next_state] ->
        {actions, _} = strip_replied(actions, :any)
        {kind, state, actions}

      _ ->
        result
    end
  end

  defp strip_replied(actions, :any) do
    {_, rest} = Enum.split_with(actions, &match?({:replied, _}, &1))
    {rest, false}
  end

  defp strip_replied(actions, from) do
    {markers, rest} = Enum.split_with(actions, &match?({:replied, _}, &1))
    {rest, Enum.any?(markers, fn {:replied, f} -> f == from end)}
  end

  defp deferred_reply?({:consistent_query, _}), do: true
  defp deferred_reply?({:consistent_aux, _}), do: true
  # A command awaiting consensus (or a pipeline notification) is answered
  # only after the entry commits and applies — potentially many events
  # later — so the initial append pass legitimately produces no reply.
  defp deferred_reply?({:command, cmd}) when is_tuple(cmd) do
    case Tuple.to_list(cmd) do
      [_, _, _, :await_consensus] -> true
      [_, _, _, {:await_consensus, _}] -> true
      [_, _, _, {:notify, _, _}] -> true
      [_, _, :await_consensus] -> true
      [_, _, {:await_consensus, _}] -> true
      [_, _, {:notify, _, _}] -> true
      _ -> false
    end
  end

  defp deferred_reply?(_), do: false

  defp has_reply_action?(actions) do
    Enum.any?(actions, fn
      {:reply, _, _, _} -> true
      {:reply, _, _} -> true
      {:reply, _} -> true
      _ -> false
    end)
  end

  # Decides whether a candidate (or pre-vote) server that the domain logic
  # mapped to `:follower` for `inner_msg` genuinely steps down. Read-only
  # operations, denied polls, and maintenance events must not abort the
  # campaign; only a higher term, proven leader traffic at our term, or
  # granting our vote to someone else does.
  defp campaign_demoted?(inner_msg, old_runtime, new_runtime) do
    old_ss = old_runtime.server_state
    new_ss = new_runtime.server_state
    old_term = Map.get(old_ss, :current_term, 0)
    new_term = Map.get(new_ss, :current_term, old_term)

    cond do
      new_term > old_term ->
        true

      gave_vote_away?(old_ss, new_ss) ->
        true

      leader_traffic?(inner_msg, old_term) ->
        true

      true ->
        false
    end
  end

  defp gave_vote_away?(_old_ss, new_ss) do
    self_id =
      try do
        Server.id(new_ss)
      rescue
        _ -> nil
      catch
        _, _ -> nil
      end

    new_voted = Map.get(new_ss, :voted_for)
    new_voted != nil and new_voted != self_id
  end

  defp leader_traffic?(%RaftEx.Domain.Types.AppendEntriesRpc{term: t}, current_term),
    do: t == current_term

  defp leader_traffic?(%RaftEx.Domain.Types.HeartbeatRpc{term: t}, current_term),
    do: t == current_term

  defp leader_traffic?(%RaftEx.Domain.Types.InstallSnapshotRpc{term: t}, current_term),
    do: t == current_term

  defp leader_traffic?(_, _), do: false

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
      {:wrap_reply, reply, replier} -> {:ok, reply, replier}
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
  rescue
    error -> {:error, rpc_error_reason(error)}
  catch
    :exit, {:timeout, _} -> :timeout
    :exit, {:noproc, _} -> {:error, :noproc}
    :exit, {{:nodedown, _}, _} -> {:error, :nodedown}
    :exit, {:shutdown, _} -> {:error, :shutdown}
    :exit, {reason, _} -> {:error, reason}
  end

  defp safe_call(server_id, msg, timeout) do
    :gen_statem.call(server_id, msg, timeout)
  rescue
    error -> {:error, rpc_error_reason(error)}
  catch
    :exit, {:timeout, _} -> :timeout
    :exit, {:noproc, _} -> {:error, :noproc}
    :exit, {{:nodedown, _}, _} -> {:error, :nodedown}
    :exit, {:shutdown, _} -> {:error, :shutdown}
    :exit, {reason, _} -> {:error, reason}
  end

  # :erpc raises (rather than exits) for some transport failures, e.g.
  # {:erpc, :noconnection}. Map them onto the same error shape as the exit
  # clauses above so callers never see a bare ErlangError.
  defp rpc_error_reason(%ErlangError{original: {:erpc, reason}}), do: reason
  defp rpc_error_reason(%ErlangError{original: reason}), do: reason
  defp rpc_error_reason(_error), do: :bad_rpc

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

    app_conf = fn key, default ->
      case Application.get_env(:raft_ex, key) do
        nil -> default
        val -> val
      end
    end

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
        wal_sync_method:
          Map.get(
            config,
            :wal_sync_method,
            app_conf.(:wal_sync_method, Map.get(sys_cfg, :wal_sync_method, :datasync))
          ),
        snapshot_chunk_size:
          Map.get(
            config,
            :snapshot_chunk_size,
            app_conf.(:snapshot_chunk_size, Map.get(sys_cfg, :snapshot_chunk_size, 65_536))
          ),
        max_pipeline_count:
          Map.get(
            config,
            :max_pipeline_count,
            app_conf.(
              :max_pipeline_count,
              Map.get(
                sys_cfg,
                :max_pipeline_count,
                Map.get(sys_cfg, :default_max_pipeline_count, 4096)
              )
            )
          ),
        max_append_entries_rpc_batch_size:
          Map.get(
            config,
            :max_append_entries_rpc_batch_size,
            app_conf.(
              :max_append_entries_rpc_batch_size,
              Map.get(
                sys_cfg,
                :max_append_entries_rpc_batch_size,
                Map.get(sys_cfg, :default_max_append_entries_rpc_batch_size, 128)
              )
            )
          ),
        max_pending_commands:
          Map.get(
            config,
            :max_pending_commands,
            app_conf.(
              :max_pending_commands,
              Map.get(sys_cfg, :max_pending_commands, 1024)
            )
          ),
        command_batch_linger_ms:
          Map.get(
            config,
            :command_batch_linger_ms,
            app_conf.(
              :command_batch_linger_ms,
              Map.get(sys_cfg, :command_batch_linger_ms, 0)
            )
          ),
        node_suspect_timeout_ms:
          Map.get(
            config,
            :node_suspect_timeout_ms,
            app_conf.(
              :node_suspect_timeout_ms,
              Map.get(sys_cfg, :node_suspect_timeout_ms, 30_000)
            )
          ),
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

    # Snapshot sends belong to a leadership term: abandon any in-flight
    # transfer when leaving the leader state so a stale send can never
    # resume under a new term. The next replication pass retries if needed.
    state =
      if raft_state != :leader and map_size(Map.get(state, :snapshot_sends, %{})) > 0 do
        Logger.debug(
          "RaftEx.App.ServerRuntime: abandoning snapshot sends on leaving leader state"
        )

        Map.put(state, :snapshot_sends, %{})
      else
        state
      end

    {state, actions} = handle_effects(raft_state, effects, :cast, %{state | server_state: ss})
    {state, drop_next_events(actions)}
  end

  # `gen_statem` forbids `next_event` actions from `:enter` callbacks. A
  # threshold crossed exactly while entering (e.g. the leader-enter noop
  # committing a backlog) drops its compaction trigger here; the next tick
  # or apply batch re-fires it from a legal context.
  defp drop_next_events(actions) do
    Enum.reject(actions, &match?({:next_event, _, _}, &1))
  end

  defp handle_leader(msg, %{server_state: ss0} = state0) do
    {next_state, ss, effects} = Server.handle_leader(msg, ss0)
    state = %{state0 | server_state: Server.persist_last_applied(ss)}
    {next_state, state, effects}
  catch
    :throw, {:delete_and_terminate, ss, effects} ->
      {:stop, delete_server_data(%{state0 | server_state: ss}), effects}

    :throw, {next_state, ss, effects} when is_atom(next_state) ->
      state = %{state0 | server_state: Server.persist_last_applied(ss)}
      {next_state, state, effects}
  end

  defp handle_follower(msg, %{server_state: ss0} = state0) do
    {next_state, ss, effects} = Server.handle_follower(msg, ss0)
    state = %{state0 | server_state: Server.persist_last_applied(ss)}
    {next_state, state, effects}
  catch
    :throw, {:delete_and_terminate, ss, effects} ->
      {:stop, delete_server_data(%{state0 | server_state: ss}), effects}
  end

  # Removes a deleted server's on-disk data directory and drops its
  # leaderboard entry so a supervisor restart or a stale record cannot
  # resurrect it. The caller then stops with :normal; with a transient child
  # spec the supervisor does not restart it, so there is no crash loop.
  defp delete_server_data(%{server_state: ss} = state) do
    data_dir =
      try do
        Map.get(ss.log, :data_dir)
      rescue
        _ -> nil
      catch
        _, _ -> nil
      end

    if is_binary(data_dir) do
      case File.rm_rf(data_dir) do
        {:ok, _} ->
          :ok

        {:error, reason, _} ->
          Logger.warning("RaftEx.App.ServerRuntime: data cleanup failed: #{inspect(reason)}")
      end
    end

    try do
      RaftEx.Leaderboard.forget_leader({Utils.ra_server_id_to_local_name(Server.id(ss)), node()})
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    state
  end

  defp handle_pre_vote_reply(
         reply,
         {from_pid, _ref},
         %{server_state: ss0, election_context: ctx0} = state0
       ) do
    # Look up peer_id from the mapping stored when RPC was sent (local peers),
    # or fall back to resolving it from the sender pid (remote peers).
    peer_id =
      case from_pid do
        {name, node} when is_atom(name) and is_atom(node) ->
          {name, node}

        pid when is_pid(pid) ->
          Process.get({:election_peer, pid}) || pid_to_server_id(pid)

        _ ->
          nil
      end

    {should_start_election, ctx, effects} =
      Election.handle_pre_vote_reply(reply, ctx0, ss0, peer_id)

    state = %{state0 | election_context: ctx}

    {state1, actions} = handle_effects(:pre_vote, effects, :cast, state)

    cond do
      should_start_election ->
        Logger.info("RaftEx.App.ServerRuntime: Pre-vote succeeded, transitioning to candidate")
        next_state(:candidate, Map.delete(state1, :pending_become), actions)

      Map.get(state1, :pending_become) == :follower ->
        {state2, actions2} =
          handle_effects(:pre_vote, [], :cast, Map.delete(state1, :pending_become))

        _ = actions2
        next_state(:follower, state2, actions)

      true ->
        {:keep_state, Map.delete(state1, :pending_become), actions}
    end
  end

  defp handle_vote_reply(
         reply,
         {from_pid, _ref},
         %{server_state: ss0, election_context: ctx0} = state0
       ) do
    # Look up peer_id from the mapping stored when RPC was sent (local peers),
    # or fall back to resolving it from the sender pid (remote peers, where no
    # tracking entry was recorded).
    peer_id =
      case from_pid do
        {name, node} when is_atom(name) and is_atom(node) ->
          {name, node}

        pid when is_pid(pid) ->
          Process.get({:election_peer, pid}) || pid_to_server_id(pid)

        _ ->
          nil
      end

    {result, ctx, effects} =
      Election.handle_vote_reply(reply, ctx0, ss0, peer_id)

    state = %{state0 | election_context: ctx}

    case result do
      :won ->
        Metrics.inc(:elections_won)
        Logger.info("RaftEx.App.ServerRuntime: Won election, becoming leader")

        {state1, actions} =
          handle_effects(:candidate, effects ++ [{:become, :leader}], :cast, state)

        next_state(:leader, Map.delete(state1, :pending_become), actions)

      :lost ->
        Metrics.inc(:elections_lost)
        Logger.info("RaftEx.App.ServerRuntime: Lost election, stepping down to follower")
        {state1, actions} = handle_effects(:candidate, effects, :cast, state)
        next_state(:follower, Map.delete(state1, :pending_become), actions)

      :ongoing ->
        {state1, actions} = handle_effects(:candidate, effects, :cast, state)
        {:keep_state, Map.delete(state1, :pending_become), actions}
    end
  end

  defp handle_election_timeout(:pre_vote, state0) do
    # Drop the previous round's token so the restarted pre-vote mints a
    # fresh one; replies from the old round must not count toward the new.
    server_state = Map.delete(state0.server_state, :pre_vote_token)

    {context, election_effects} =
      Election.start_election(:pre_vote, server_state)

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

  # Replies are sent synchronously (a plain `:gen_statem.reply/2`) so they
  # are delivered even on paths that cannot return gen_statem actions (such
  # as `{:stop, ...}`). Each send records a `{:replied, from}` marker so
  # `ensure_call_replied/3` can tell a replied call from a dropped one; the
  # markers are stripped before anything is returned to `gen_statem`.
  defp handle_effect(:leader, {:reply, from, reply}, _, state, actions) do
    :gen_statem.reply(from, reply)
    {state, [{:replied, from} | actions]}
  end

  defp handle_effect(_, {:reply, reply}, {:call, from}, state, actions) do
    :gen_statem.reply(from, reply)
    {state, [{:replied, from} | actions]}
  end

  # Replies routed to a non-default replier (`reply_from: :local` or
  # `{:member, id}`): answer the waiting caller and identify who replied.
  # The effect carries the same single-wrapped payload as a plain reply;
  # unwrap it once so the caller sees exactly one `wrap_reply` layer.
  defp handle_effect(_, {:reply, from, {:wrap_reply, reply}, replier}, _, state, actions) do
    :gen_statem.reply(from, {:wrap_reply, reply, replier})
    {state, [{:replied, from} | actions]}
  end

  defp handle_effect(_, {:reply, from, {:wrap_reply, _} = wrapped}, _, state, actions) do
    :gen_statem.reply(from, wrapped)
    {state, [{:replied, from} | actions]}
  end

  defp handle_effect(_, {:reply, from, reply}, _, state, actions) do
    :gen_statem.reply(from, {:wrap_reply, reply})
    {state, [{:replied, from} | actions]}
  end

  # Pipeline notifications: exactly one message per correlation id.
  defp handle_effect(_, {:notify, notifys}, _, state, actions) when is_map(notifys) do
    Enum.each(notifys, fn {pid, corrs} ->
      Enum.each(List.wrap(corrs), fn {corr, reply} ->
        send(pid, {:pipeline_notify, corr, reply})
      end)
    end)

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

            state =
              state0
              |> Map.put(:snapshot_sends, Map.put(sends, peer_id, send_state))

            state = update_peer_status(state, peer_id, {:sending_snapshot, self(), 0})

            {state, actions} =
              send_next_snapshot_chunk(peer_id, send_state, state, actions)

            {state, [snapshot_send_timeout(state, peer_id) | actions]}

          _err ->
            {state0, actions}
        end
    end
  end

  defp handle_effect(:leader, {:clear_snapshot_send, peer_id}, _, state0, actions) do
    sends = Map.delete(Map.get(state0, :snapshot_sends, %{}), peer_id)
    state = Map.put(state0, :snapshot_sends, sends)
    state = update_peer_status(state, peer_id, :normal)
    {state, actions}
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

  # AppendEntries sends are optimistic: the peer's next_index advances when
  # the RPC is built. If the send itself fails (partitioned or not-yet-up
  # peer), roll the peer back to the RPC's prev index so the next tick or
  # ack-triggered pass retries from a valid position instead of stranding
  # the peer ahead with nothing in flight.
  defp handle_effect(
         _,
         {:send_rpc, to, %RaftEx.Domain.Types.AppendEntriesRpc{prev_log_index: prev} = rpc},
         _,
         state,
         actions
       ) do
    case send_rpc(to, rpc, state) do
      :ok ->
        {state, actions}

      {:error, _} ->
        {rollback_peer_next_index(state, to, prev), actions}
    end
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

    result =
      try do
        LogMeta.store_sync(meta_name, uid, key, value)
      catch
        _, reason -> {:error, reason}
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("RaftEx.ServerRuntime: store_meta #{key} failed: #{inspect(reason)}")
    end

    {state, actions}
  end

  defp handle_effect(_, {:update_term, term}, _, %{server_state: ss} = state, actions) do
    meta_name =
      case ss do
        %{cfg: %{system_config: sc}} -> meta_name_from_sc(sc)
        _ -> nil
      end

    uid =
      case ss do
        %{cfg: %{uid: u}} when is_binary(u) -> u
        _ -> nil
      end

    if is_atom(meta_name) and is_binary(uid) do
      try do
        LogMeta.store_sync(meta_name, uid, :current_term, term)
      catch
        _, _ -> :ok
      end

      try do
        LogMeta.store_sync(meta_name, uid, :voted_for, nil)
      catch
        _, _ -> :ok
      end
    end

    new_ss =
      ss
      |> Map.put(:current_term, term)
      |> Map.put(:voted_for, nil)
      |> Map.put(:leader_id, nil)
      |> Map.put(:transfer_leadership, nil)

    {%{state | server_state: new_ss}, actions}
  end

  defp handle_effect(_, {:become, role}, _, state, actions)
       when role in [:follower, :candidate, :leader, :pre_vote] do
    {Map.put(state, :pending_become, role), actions}
  end

  defp handle_effect(_, {:reset_election_timeout}, _, state, actions) do
    set_election_timeout(state, actions)
  end

  defp handle_effect(_, {:transfer_leadership_timeout, _target}, _, state, actions) do
    {state, [{:state_timeout, 1_000, :clear_transfer_leadership} | actions]}
  end

  defp handle_effect(_, _, _, state, actions), do: {state, actions}

  # Roll a peer's next_index back to a confirmed-sent position after a
  # failed AppendEntries send, so the next tick or ack-triggered pass
  # retries from valid state instead of stranding the peer.
  defp rollback_peer_next_index(%{server_state: ss} = state, peer_id, prev_idx) do
    if is_map(ss.cluster) and Map.has_key?(ss.cluster, peer_id) do
      peer = Map.get(ss.cluster, peer_id)
      current_ni = Map.get(peer, :next_index, prev_idx + 1)
      ni = min(current_ni, prev_idx + 1)

      in_flight =
        case Map.get(peer, :in_flight, :queue.new()) do
          q when is_tuple(q) ->
            :queue.filter(fn {p, _l} -> p <= prev_idx end, q)

          list when is_list(list) ->
            :queue.from_list(Enum.filter(list, fn {p, _l} -> p <= prev_idx end))

          _ ->
            :queue.new()
        end

      new_ss = %{
        ss
        | cluster:
            Cluster.update_peer(
              peer_id,
              %{next_index: max(ni, 1), in_flight: in_flight},
              ss.cluster
            )
      }

      %{state | server_state: new_ss}
    else
      state
    end
  end

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
        state = Map.put(state0, :snapshot_sends, sends)
        state = update_peer_status(state, peer_id, :normal)
        {state, actions}

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
        {state, [{:next_event, :info, {:send_snapshot_chunk, peer_id}} | actions]}

      {:error, _reason} ->
        sends = Map.delete(Map.get(state0, :snapshot_sends, %{}), peer_id)
        state = Map.put(state0, :snapshot_sends, sends)
        state = update_peer_status(state, peer_id, :normal)
        {state, actions}
    end
  end

  defp send_snapshot_rpc({name, node} = _peer_id, rpc, state)
       when node == node() do
    sender = id(state)

    case Process.whereis(name) do
      nil -> :ok
      pid -> :gen_statem.cast(pid, {self(), {:rpc, sender, rpc}})
    end
  end

  defp send_snapshot_rpc(peer_id, rpc, state) do
    Transport.send_rpc(peer_id, rpc, sender: id(state))
  end

  defp snapshot_dir(%{server_state: %{log: %{data_dir: dir}}}) when is_binary(dir) do
    Path.join(dir, "snapshot")
  end

  defp snapshot_dir(_state) do
    Path.join(System.tmp_dir!(), "raft_ex_snapshot")
  end

  defp update_peer_status(state, peer_id, status) do
    ss = state.server_state

    if is_map(ss.cluster) and Map.has_key?(ss.cluster, peer_id) do
      new_ss = %{ss | cluster: Cluster.update_peer(peer_id, %{status: status}, ss.cluster)}
      %{state | server_state: new_ss}
    else
      state
    end
  end

  # Network topology events (10.6). A node-down marks that node's peers
  # suspect so no AppendEntries/heartbeat is wasted on them and the leader
  # keeps serving reachable peers; a node-up restores them and schedules an
  # immediate reconciliation pass. Suspects also age out via the tick reap
  # below in case a node-up event is missed.
  defp handle_network_event(_raft_state, :nodedown, node, state) do
    since = System.monotonic_time(:millisecond)
    {:keep_state, mark_node_peers(state, node, {:suspect, since}), []}
  end

  defp handle_network_event(:leader, :nodeup, node, state) do
    state = mark_node_peers(state, node, :normal)

    {state, actions} =
      handle_effects(:leader, [{:next_event, :info, :pipeline_rpcs}], :cast, state)

    {:keep_state, state, actions}
  end

  defp handle_network_event(_raft_state, :nodeup, node, state) do
    {:keep_state, mark_node_peers(state, node, :normal), []}
  end

  defp handle_network_event(_raft_state, _event, _node, state) do
    {:keep_state, state, []}
  end

  defp mark_node_peers(%{server_state: %{cfg: %{id: self_id}} = ss} = state, node, status) do
    if is_map(ss.cluster) do
      cluster =
        Enum.reduce(ss.cluster, ss.cluster, fn
          {{_name, ^node} = peer_id, _peer}, acc when peer_id != self_id ->
            Cluster.update_peer(peer_id, %{status: status}, acc)

          _, acc ->
            acc
        end)

      %{state | server_state: %{ss | cluster: cluster}}
    else
      state
    end
  end

  defp mark_node_peers(state, _node, _status), do: state

  # Retry deadline for suspect peers (10.2): a suspect older than the
  # configured timeout is probed again instead of being skipped forever.
  defp reap_suspects(%{server_state: ss} = state) do
    timeout = Map.get(state.conf, :node_suspect_timeout_ms, 30_000)
    now = System.monotonic_time(:millisecond)

    if is_map(ss.cluster) do
      cluster =
        Enum.reduce(ss.cluster, ss.cluster, fn
          {peer_id, %{status: {:suspect, since}}}, acc when now - since > timeout ->
            Cluster.update_peer(peer_id, %{status: :normal}, acc)

          _, acc ->
            acc
        end)

      %{state | server_state: %{ss | cluster: cluster}}
    else
      state
    end
  end

  defp meta_name_from_sc(%{names: %{log_meta: name}}), do: name
  defp meta_name_from_sc(sc), do: Server.meta_name(sc)

  defp send_rpc({name, node} = to, msg, state) do
    incr_counter(state.conf, :rpcs_sent, 1)
    sender = id(state)

    if node == node() do
      case Process.whereis(name) do
        nil -> :ok
        pid -> :gen_statem.cast(pid, {self(), {:rpc, sender, msg}})
      end
    else
      Transport.send_rpc(to, msg, sender: sender)
    end
  end

  defp send_rpc_with_tracking(peer_id, rpc, state) do
    incr_counter(state.conf, :rpcs_sent, 1)
    {name, node} = peer_id
    sender = id(state)

    if node == node() do
      case Process.whereis(name) do
        nil ->
          :ok

        pid ->
          # Track this peer for reply correlation
          Process.put({:election_peer, pid}, peer_id)
          :gen_statem.cast(pid, {self(), {:rpc, sender, rpc}})
      end
    else
      Transport.send_rpc(peer_id, rpc, sender: sender)
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

  defp next_state(:leader, state, actions) do
    pending = Map.get(state, :pending_commands, [])
    next_events = Enum.map(pending, fn {f, cmd} -> {:next_event, {:call, f}, cmd} end)

    state = Map.merge(state, %{election_timeout_set: false, pending_commands: []})

    # A batch queued while the WAL was down (or during the transition back
    # to leader) must still be flushed.
    state =
      if :queue.is_empty(Map.get(state, :cmd_batch_queue, :queue.new())) do
        state
      else
        schedule_batch_flush(state)
      end

    {:next_state, :leader, state, actions ++ next_events}
  end

  defp next_state(next, state, actions) do
    q = Map.get(state, :cmd_batch_queue, :queue.new())

    {state, actions} =
      if :queue.is_empty(q) do
        {state, actions}
      else
        # Leaving leadership with unflushed commands: fail every queued
        # caller fast instead of hanging them.
        replies =
          for cmd <- :queue.to_list(q),
              match?({_, %{from: from}, _, _} when from != nil, cmd) do
            {_, %{from: from}, _, _} = cmd
            {:reply, from, {:error, :not_leader}}
          end

        state =
          Map.merge(state, %{cmd_batch_queue: :queue.new(), batch_flush_scheduled?: false})

        {state, actions ++ replies}
      end

    {:next_state, next, Map.merge(state, %{election_timeout_set: false}), actions}
  end

  defp make_rpcs(%{server_state: ss0} = state) do
    {ss, rpcs} = Server.make_rpcs(ss0)
    {%{state | server_state: ss}, rpcs}
  end

  defp make_pipeline_rpcs(%{server_state: ss0} = state) do
    {ss, rpcs} = Server.make_pipeline_rpcs(ss0)
    {%{state | server_state: ss}, rpcs}
  end

  defp do_state_query(spec, %{server_state: ss}), do: Server.state_query(spec, ss)
  defp id(%{server_state: ss}), do: Server.id(ss)
  defp record_cluster_change(_state), do: :ok
  defp incr_counter(_, _, _), do: :ok
  # Peel transport envelopes, returning `{sender_id_or_nil, payload}`.
  # The `{:rpc, sender, ...}` envelope carries the sender's server id so it
  # can be resolved locally without a remote `:erpc` lookup (10.3).
  defp peel_envelope({:rpc, sender, msg})
       when is_tuple(sender) and tuple_size(sender) == 2,
       do: {sender, peel_envelope_payload(msg)}

  defp peel_envelope({:rpc, msg}), do: {nil, peel_envelope_payload(msg)}
  defp peel_envelope({:gen_cast, msg}), do: peel_envelope(msg)
  defp peel_envelope(msg), do: {nil, msg}

  defp peel_envelope_payload({:gen_cast, msg}), do: peel_envelope_payload(msg)
  defp peel_envelope_payload(msg), do: msg

  defp unwrap_from({from, msg}) when is_pid(from) do
    {sender, inner} = peel_envelope(msg)
    {sender || from, inner}
  end

  defp unwrap_from(msg) do
    peel_envelope(msg)
  end

  # Resolve a sender reference to a server id: envelope ids pass through,
  # pids go through the local cache (falling back to a lookup), and anything
  # else falls back to the RPC payload's own leader/candidate id (10.4).
  # Returns `nil` when the sender cannot be identified at all.
  defp resolve_sender(sender, inner_msg) do
    case sender do
      {name, node} when is_atom(name) and is_atom(node) -> sender
      pid when is_pid(pid) -> pid_to_server_id(pid)
      _ -> nil
    end || sender_id_from_payload(inner_msg)
  end

  defp sender_id_from_payload(%{leader_id: {name, node}})
       when is_atom(name) and is_atom(node),
       do: {name, node}

  defp sender_id_from_payload(%{candidate_id: {name, node}})
       when is_atom(name) and is_atom(node),
       do: {name, node}

  defp sender_id_from_payload(_), do: nil

  defp pid_to_server_id(pid) do
    pid_node = node(pid)

    case pid_cache_lookup(pid) do
      nil -> pid_to_server_id_uncached(pid, pid_node)
      id -> id
    end
  end

  # Local pid → server-id cache (10.4): registered names are stable per
  # server, so a cached mapping stays valid for the process lifetime.
  defp pid_cache_lookup(pid) do
    case :ets.lookup(:ra_pid_to_server_id, pid) do
      [{_, id}] -> id
      [] -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp pid_cache_store(pid, {name, node} = id)
       when is_atom(name) and is_atom(node) do
    try do
      :ets.insert(:ra_pid_to_server_id, {pid, id})
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    id
  end

  defp pid_cache_store(_pid, _), do: nil

  defp pid_to_server_id_uncached(pid, pid_node) do
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
      {:registered_name, name} when is_atom(name) -> pid_cache_store(pid, {name, pid_node})
      [{:registered_name, name}] when is_atom(name) -> pid_cache_store(pid, {name, pid_node})
      _ -> nil
    end
  end

  defp set_tick_timer(%{conf: %{tick_timeout: tick_timeout}}, actions) do
    [{{:timeout, :tick}, tick_timeout, :tick_timeout} | actions]
  end
end
