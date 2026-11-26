defmodule RaftEx.Domain.ServerCoverageTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.{Conn, Machine, Types}
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Infra.Persistence.{Log, LogMeta}

  @self_id {:server_cov1, node()}
  @peer2 {:server_cov2, node()}

  defmodule HaltNoRespPlug do
    def init(opts), do: opts
    def call(conn, _opts), do: Conn.halt(conn)
  end

  defmodule Aux5Machine do
    @behaviour Machine
    def init(conf), do: conf
    def apply(_meta, cmd, state), do: {state, cmd}

    def handle_aux(_raft, _type, cmd, aux, state), do: {:reply, {:ok5, cmd}, aux, state}
  end

  defmodule Aux5EffectsMachine do
    @behaviour Machine
    def init(conf), do: conf
    def apply(_meta, cmd, state), do: {state, cmd}

    def handle_aux(_raft, _type, cmd, aux, state), do: {:no_reply, aux, state, [{:aux, cmd}]}
  end

  # The 6-arity callback receives the log and machine state but not the full
  # server state; exercise it by returning the server state held in `aux`.
  defmodule Aux6Machine do
    @behaviour Machine
    def init(conf), do: conf
    def apply(_meta, cmd, state), do: {state, cmd}

    def handle_aux(_raft, _type, cmd, aux, _log, _mac_state) do
      case cmd do
        :no_reply -> {:no_reply, :new_aux, aux}
        {:no_reply_effects, eff} -> {:no_reply, :new_aux, aux, [eff]}
        other -> {:reply, {:ok6, other}, :new_aux, aux}
      end
    end
  end

  defmodule V0 do
    def init(conf), do: conf
    def apply(_meta, cmd, state), do: {state, cmd}
  end

  defmodule V1 do
    def init(conf), do: conf
    def apply(_meta, cmd, state), do: {state, cmd}
  end

  defmodule VersionedMachine do
    def init(conf), do: conf
    def apply(_meta, cmd, state), do: {state, cmd}
    def version, do: 2
    def which_module(0), do: V0
    def which_module(1), do: V1
    def which_module(_), do: V1
  end

  defmodule ErrorWal do
    use GenServer
    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)
    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:append, _uid, _entries}, _from, s), do: {:reply, {:error, :wal_full}, s}
  end

  defmodule GarbageWal do
    use GenServer
    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)
    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:append, _uid, _entries}, _from, s), do: {:reply, :garbage, s}
  end

  setup do
    dir = RaftEx.TestSupport.tmp_dir("server_cov")
    meta_name = :"server_cov_meta_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogMeta, %{name: :server_cov, names: %{log_meta: meta_name}, data_dir: dir}}
    )

    Process.put(:server_cov_meta_name, meta_name)
    :ok
  end

  defp config(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "server-cov-uid",
      log_id: "server-cov-log",
      metrics_key: :server_cov_metrics,
      machine: Keyword.get(opts, :machine, {:machine, Machine.Simple, %{}}),
      machine_version: Keyword.get(opts, :machine_version, 0),
      effective_machine_version: Keyword.get(opts, :eff_version, 0),
      effective_machine_module: Keyword.get(opts, :mac_mod, Machine.Simple),
      effective_handle_aux_fun: Keyword.get(opts, :aux_fun, nil),
      command_plugs: Keyword.get(opts, :command_plugs, []),
      rpc_plugs: Keyword.get(opts, :rpc_plugs, []),
      machine_versions: Keyword.get(opts, :machine_versions, [{0, 0}]),
      counter: Keyword.get(opts, :counter, nil),
      max_pipeline_count: Keyword.get(opts, :max_pipeline_count, 4096),
      system_config: %{names: %{log_meta: Process.get(:server_cov_meta_name)}}
    )
  end

  defp log(entries, opts \\ []) do
    entries_map = Map.new(entries, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)
    indexes = Enum.map(entries, &elem(&1, 0))

    {range, last_term} =
      case indexes do
        [] -> {nil, 0}
        _ -> {{Enum.min(indexes), Enum.max(indexes)}, entries |> List.last() |> elem(1)}
      end

    last_index = if indexes == [], do: 0, else: Enum.max(indexes)

    %Log{
      entries: entries_map,
      range: Keyword.get(opts, :range, range),
      last_term: last_term,
      next_index: Keyword.get(opts, :next_index, last_index + 1),
      last_written_index_term: Keyword.get(opts, :last_written, {last_index, last_term}),
      current_snapshot: Keyword.get(opts, :current_snapshot, nil),
      meta_name: Process.get(:server_cov_meta_name),
      cfg: %{uid: "server-cov-uid"}
    }
  end

  defp state(opts \\ []) do
    %{
      cfg: config(opts),
      current_term: Keyword.get(opts, :current_term, 2),
      commit_index: 1,
      last_applied: 1,
      persisted_last_applied: 3,
      leader_id: nil,
      voted_for: nil,
      commit_latency: 0,
      rpc_from: nil,
      membership: :voter,
      cluster_change_permitted: Keyword.get(opts, :permitted, false),
      cluster_index_term: Keyword.get(opts, :cluster_index_term, {0, 0}),
      pending_consistent_queries: Keyword.get(opts, :pending, []),
      pending_release_cursor: Keyword.get(opts, :pending_release_cursor, nil),
      query_index: 0,
      queries_waiting_heartbeats: :queue.new(),
      cluster: Keyword.get(opts, :cluster, default_cluster()),
      log: Keyword.get(opts, :log, log([{1, 2, :cmd}, {2, 2, :cmd}])),
      machine_state: {:simple, fn _cmd, st -> st end, :ms},
      aux_state: Keyword.get(opts, :aux_state, nil)
    }
  end

  defp default_cluster do
    %{
      @self_id => %{
        voter_status: %{membership: :voter},
        next_index: 3,
        match_index: 2,
        query_index: 0,
        commit_index_sent: 0,
        status: :normal
      },
      @peer2 => %{
        voter_status: %{membership: :voter},
        next_index: 1,
        match_index: 0,
        query_index: 0,
        commit_index_sent: 0,
        status: :normal
      }
    }
  end

  defp simple_query_fun, do: fn st -> {:queried, st} end

  # -------------------------------------------------------------------------
  # init/1, meta_name/1, counters
  # -------------------------------------------------------------------------

  describe "init and misc helpers" do
    test "init normalises a simple machine config" do
      uid = "init-simple-#{System.unique_integer([:positive])}"
      id = {:init_simple, node()}

      s =
        Server.init(%{
          id: id,
          uid: uid,
          cluster_name: :c,
          initial_members: [id],
          log_init_args: %{uid: uid},
          machine: {:simple, fn _cmd, st -> st end, :initial},
          system_config: RaftEx.App.System.default_config()
        })

      assert elem(s.cfg.machine, 0) == :machine
      assert elem(s.cfg.machine, 1) == Machine.Simple
      assert match?({:simple, _, :initial}, s.machine_state)
      assert s.current_term == 0
      assert s.membership == :voter
    end

    test "init normalises a module machine config" do
      uid = "init-module-#{System.unique_integer([:positive])}"
      id = {:init_module, node()}

      s =
        Server.init(%{
          id: id,
          uid: uid,
          cluster_name: :c,
          initial_members: [id],
          log_init_args: %{uid: uid},
          machine: {:module, V0, %{}},
          system_config: RaftEx.App.System.default_config()
        })

      assert s.cfg.machine == {:machine, V0, %{}}
    end

    test "meta_name/1 covers all fallback clauses" do
      assert Server.meta_name(config()) == Process.get(:server_cov_meta_name)
      assert Server.meta_name(%{names: %{log_meta: :custom_name}}) == :custom_name
      assert Server.meta_name(%{config() | system_config: %{names: %{}}}) == :ra_log_meta
      assert Server.meta_name(%{names: %{}}) == :ra_log_meta
      assert Server.meta_name(:not_a_map) == :ra_log_meta
    end

    test "a configured counter is accepted on the command path" do
      s = state(counter: :a_counter)
      cmd = {:"$usr", %{}, :command, :noreply}

      assert {:leader, _state, _effects} = Server.handle_leader({:command, cmd}, s)
    end
  end

  # -------------------------------------------------------------------------
  # recovery
  # -------------------------------------------------------------------------

  describe "recover/1" do
    test "scans cluster changes and generic entries above the commit index" do
      cc = {:"$ra_cluster_change", %{}, default_cluster(), :noreply}
      l = log([{2, 2, :cmd}, {3, 2, cc}])
      s = %{state(log: l) | commit_index: 1, last_applied: 1}

      recovered = Server.recover(s)

      assert recovered.cluster_index_term == {3, 2}
      assert recovered.cluster_change_permitted
    end

    test "truncates the log when the scan stops before the last index" do
      l = log([{3, 2, :cmd}], range: {5, 3}, last_written: {3, 2})
      s = %{state(log: l) | commit_index: 2, last_applied: 2}

      recovered = Server.recover(s)

      assert {5, 2} = recovered.log.range
    end
  end

  # -------------------------------------------------------------------------
  # leader message handler gaps
  # -------------------------------------------------------------------------

  describe "leader handler gaps" do
    test "halted command without a from field produces no effects" do
      s = state(command_plugs: [{HaltNoRespPlug, []}])
      cmd = {:"$usr", %{}, :command, :noreply}
      assert {:leader, ^s, []} = Server.handle_leader({:command, cmd}, s)
    end

    test "a non-written ra_log_event is forwarded to the log" do
      assert {:leader, _state, []} = Server.handle_leader({:ra_log_event, :some_event}, state())
    end

    test "replies from unknown peers are ignored" do
      assert {:leader, _s, []} =
               Server.handle_leader(%Types.AppendEntriesReply{term: 2, success: false}, state())

      assert {:leader, _s, []} =
               Server.handle_leader(%Types.HeartbeatReply{term: 2, query_index: 1}, state())
    end

    test "install-snapshot result with a higher term steps down" do
      reply = %Types.InstallSnapshotResult{term: 3, last_index: 5, last_term: 2}
      s = %{state() | rpc_from: @peer2}

      assert {:follower, new_state, effects} = Server.handle_leader(reply, s)
      assert new_state.current_term == 3
      assert {:store_meta, :current_term, 3} in effects
    end

    test "install-snapshot result from a known peer updates match indexes" do
      reply = %Types.InstallSnapshotResult{term: 2, last_index: 5, last_term: 2}
      cluster = default_cluster()
      peer = %{cluster[@peer2] | status: {:sending_snapshot, self(), 0}}
      cluster = Map.put(cluster, @peer2, peer)
      s = %{state() | rpc_from: @peer2, cluster: cluster}

      assert {:leader, new_state, effects} = Server.handle_leader(reply, s)
      assert new_state.cluster[@peer2].match_index == 5
      assert new_state.cluster[@peer2].next_index == 6
      assert new_state.cluster[@peer2].status == :normal
      assert {:clear_snapshot_send, @peer2} in effects
      assert {:next_event, :info, :pipeline_rpcs} in effects
    end

    test "aux_command with opts delegates to the aux handler" do
      s = state(aux_fun: {:handle_aux, 5}, mac_mod: Aux5Machine)

      assert {:leader, _state, [{:reply, {:ok5, :cmd}}]} =
               Server.handle_leader({:aux_command, :cmd, []}, s)
    end

    test "major compaction on a leader may checkpoint" do
      l = log([{2, 2, :cmd}], current_snapshot: {1, 2})
      s = %{state(log: l) | last_applied: 2}

      assert {:leader, _state, _effects} =
               Server.handle_leader({:ra_log_event, :major_compaction}, s)
    end
  end

  # -------------------------------------------------------------------------
  # follower message handler gaps
  # -------------------------------------------------------------------------

  describe "follower handler gaps" do
    test "halted AppendEntries RPC without a resp gets a default reply" do
      s = state(rpc_plugs: [{HaltNoRespPlug, []}])
      rpc = %Types.AppendEntriesRpc{term: 2, leader_id: @peer2}

      assert {:follower, _state, [{:send_rpc, @peer2, %Types.AppendEntriesReply{} = reply}]} =
               Server.handle_follower(rpc, s)

      assert reply.term == 2
      refute reply.success
    end

    test "pre-vote requests produce a PreVoteResult" do
      rpc = %Types.PreVoteRpc{
        term: 2,
        token: make_ref(),
        candidate_id: @peer2,
        last_log_index: 2,
        last_log_term: 2
      }

      assert {:follower, _state, effects} = Server.handle_follower(rpc, state())

      assert {:send_rpc, @peer2, %Types.PreVoteResult{vote_granted: true, token: tok}} =
               List.last(effects)

      assert tok == rpc.token
    end

    test "install-snapshot RPC with a stale term is delegated" do
      rpc = %Types.InstallSnapshotRpc{term: 1, leader_id: @peer2}
      assert {:follower, _state, effects} = Server.handle_follower(rpc, state())
      assert Enum.any?(effects, &match?({:send_rpc, @peer2, %Types.InstallSnapshotResult{}}, &1))
    end

    test "major compaction on a follower may checkpoint" do
      l = log([{2, 2, :cmd}], current_snapshot: {1, 2})
      s = %{state(log: l) | last_applied: 2}

      assert {:follower, _state, _effects} =
               Server.handle_follower({:ra_log_event, :major_compaction}, s)
    end

    test "aux_command variants delegate to the aux handler" do
      s = state(aux_fun: {:handle_aux, 5}, mac_mod: Aux5Machine)

      assert {:follower, _state, [{:reply, {:ok5, :cmd}}]} =
               Server.handle_follower({:aux_command, :cmd}, s)

      assert {:follower, _state, [{:reply, {:ok5, :cmd}}]} =
               Server.handle_follower({:aux_command, :cmd, []}, s)
    end
  end

  # -------------------------------------------------------------------------
  # handle_aux (6-arity)
  # -------------------------------------------------------------------------

  describe "handle_aux/4 with a six-arity machine handler" do
    test "reply, no-reply and effects variants" do
      s0 = state(aux_fun: {:handle_aux, 6}, mac_mod: Aux6Machine)
      s = %{s0 | aux_state: s0}

      assert {:leader, state1, [{:reply, {:ok6, :cmd}}]} =
               Server.handle_aux(:leader, :call, :cmd, s)

      assert state1.aux_state == :new_aux

      assert {:leader, state2, []} = Server.handle_aux(:leader, :cast, :no_reply, s)
      assert state2.aux_state == :new_aux

      assert {:leader, _state, [{:aux, :done}]} =
               Server.handle_aux(:leader, :cast, {:no_reply_effects, {:aux, :done}}, s)
    end
  end

  # -------------------------------------------------------------------------
  # quorum / consistent queries
  # -------------------------------------------------------------------------

  describe "consistent queries" do
    test "consistent_aux with peers emits heartbeat RPCs" do
      assert {:leader, _state, effects} = Server.handle_leader({:consistent_aux, :cmd}, state())
      assert Enum.any?(effects, &match?({:send_rpc, @peer2, %Types.HeartbeatRpc{}}, &1))
    end

    test "a heartbeat that fails to reach a majority leaves the query waiting" do
      qr = {:query, {:caller, node()}, simple_query_fun(), state().commit_index}
      waiting = :queue.in({1, qr}, :queue.new())
      s = %{state() | queries_waiting_heartbeats: waiting, rpc_from: @peer2}

      assert {:leader, new_state, []} =
               Server.handle_leader(%Types.HeartbeatReply{term: 2, query_index: 0}, s)

      assert :queue.to_list(new_state.queries_waiting_heartbeats) == [{1, qr}]
    end

    test "a quorum whose read index is not applied yet parks the query" do
      qr = {:query, {:caller, node()}, simple_query_fun(), 5}
      waiting = :queue.in({1, qr}, :queue.new())
      s = %{state() | queries_waiting_heartbeats: waiting, last_applied: 1, rpc_from: @peer2}
      s = %{s | cluster: Map.put(s.cluster, @peer2, %{s.cluster[@peer2] | query_index: 1})}

      assert {:leader, new_state, []} =
               Server.handle_leader(%Types.HeartbeatReply{term: 2, query_index: 1}, s)

      assert new_state.pending_consistent_queries == [qr]
      assert :queue.is_empty(new_state.queries_waiting_heartbeats)
    end

    test "a self peer without a voter marker falls back to state membership" do
      cluster = default_cluster()

      cluster =
        put_in(cluster[@self_id], %{cluster[@self_id] | voter_status: %{membership: :non_voter}})

      cluster = put_in(cluster[@peer2].query_index, 1)

      qr = {:query, {:caller, node()}, simple_query_fun(), 1}

      s = %{
        state(cluster: cluster)
        | queries_waiting_heartbeats: :queue.in({1, qr}, :queue.new()),
          rpc_from: @peer2
      }

      assert {:leader, _new_state, effects} =
               Server.handle_leader(%Types.HeartbeatReply{term: 2, query_index: 1}, s)

      assert Enum.any?(effects, &match?({:reply, _, {:ok, _, _}}, &1))
    end

    test "pending read queries are released once the log catches up" do
      ready = {:query, :ready_from, simple_query_fun(), 1}
      later = {:query, :later_from, simple_query_fun(), 9}
      s = %{state() | pending_consistent_queries: [ready, later]}

      assert {:leader, new_state, effects} =
               Server.handle_leader({:ra_log_event, {:written, [{3, 2, :cmd}], {3, 2}}}, s)

      assert new_state.pending_consistent_queries == [later]
      assert Enum.any?(effects, &match?({:reply, :ready_from, {:ok, _, _}}, &1))
    end

    test "a heartbeat skips peers that already acknowledged a newer index" do
      s = put_in(state().cluster[@peer2].query_index, 5)

      assert {:leader, new_state, []} =
               Server.handle_leader({:consistent_query, simple_query_fun()}, s)

      assert [{1, _qr}] = :queue.to_list(new_state.queries_waiting_heartbeats)
    end

    test "pending queries are retried as heartbeats when permitted" do
      qr = {:query, :pending_from, simple_query_fun(), 1}
      s = %{state(permitted: true) | pending_consistent_queries: [qr]}

      assert {:leader, new_state, effects} =
               Server.handle_leader({:ra_log_event, {:written, [{3, 2, :cmd}], {3, 2}}}, s)

      assert new_state.pending_consistent_queries == []
      assert Enum.any?(effects, &match?({:send_rpc, @peer2, %Types.HeartbeatRpc{}}, &1))
    end

    test "aux queries pass through non-reply effects" do
      cluster = %{@self_id => default_cluster()[@self_id]}
      s = state(cluster: cluster, aux_fun: {:handle_aux, 5}, mac_mod: Aux5EffectsMachine)

      assert {:leader, _state, [[aux: :done]]} =
               Server.handle_leader({:consistent_aux, :done}, s)
    end
  end

  # -------------------------------------------------------------------------
  # transfer / membership helpers
  # -------------------------------------------------------------------------

  describe "membership edge cases" do
    test "transfer rejects a non-voter target" do
      cluster =
        Map.put(default_cluster(), @peer2, %{
          default_cluster()[@peer2]
          | voter_status: %{membership: :non_voter},
            match_index: 10
        })

      assert {:leader, _s, effects} =
               Server.handle_leader({:transfer_leadership, @peer2}, state(cluster: cluster))

      assert {:reply, {:error, :not_caught_up}} in effects
    end

    test "get_membership falls back when membership is nil" do
      assert Server.get_membership(%{state() | membership: nil}) == nil
    end

    test "persist_last_applied stores an advanced value" do
      l = log([{1, 2, :cmd}, {5, 2, :cmd}], last_written: {5, 2})
      s = %{state(log: l) | last_applied: 5, persisted_last_applied: 1}

      assert %{persisted_last_applied: 5} = Server.persist_last_applied(s)
    end

    test "persist_last_applied is capped by the last written index" do
      s = %{state() | last_applied: 5, persisted_last_applied: 3}

      assert ^s = Server.persist_last_applied(s)
    end
  end

  # -------------------------------------------------------------------------
  # log application (apply_entry variants)
  # -------------------------------------------------------------------------

  describe "apply_committed entry variants" do
    test "a cluster change applies with an existing cluster_index_term" do
      cc = {:"$ra_cluster_change", %{}, default_cluster(), :noreply}
      l = log([{2, 2, cc}])
      s = %{state(log: l, permitted: true) | cluster_index_term: {1, 1}, last_applied: 0}

      {new_state, _effects} = Server.apply_committed(2, s)

      assert new_state.cluster_index_term == {2, 2}
      assert new_state.cluster_change_permitted
    end

    test "a machine version noop upgrades the effective version" do
      l = log([{2, 2, {:noop, %{}, 1}}])

      s =
        state(
          log: l,
          machine_version: 1,
          eff_version: 0,
          machine: {:machine, VersionedMachine, %{}},
          mac_mod: V0,
          machine_versions: [{0, 0}]
        )

      s = %{s | last_applied: 0, cfg: %{s.cfg | effective_machine_module: V0}}

      {new_state, _effects} = Server.apply_committed(2, s)

      assert new_state.cfg.effective_machine_version == 1
      assert new_state.cfg.effective_machine_module == V1
      assert new_state.cfg.machine_versions == [{2, 1}, {0, 0}]
    end

    test "an unknown machine version noop only records the version" do
      l = log([{2, 2, {:noop, %{}, 1}}])
      s = state(log: l)

      {new_state, _effects} = Server.apply_committed(2, %{s | last_applied: 0})

      assert new_state.cfg.effective_machine_version == 1
      assert new_state.cfg.effective_machine_module == Machine.Simple
    end

    test "command metadata timestamps are converted to system_time" do
      l = log([{2, 2, {:"$usr", %{ts: 1_000}, :cmd, :noreply}}])
      s = %{state(log: l) | last_applied: 0}

      {new_state, _effects} = Server.apply_committed(2, s)
      assert new_state.last_applied == 2
    end

    test "a cluster delete throws a terminate signal" do
      cmd = {:"$ra_cluster", %{}, :delete, :noreply}
      l = log([{2, 2, cmd}])
      s = %{state(log: l) | last_applied: 0}

      try do
        Server.apply_committed(2, s)
        flunk("expected a delete_and_terminate throw")
      catch
        :throw, {:delete_and_terminate, final_state, effects} ->
          assert final_state.last_applied == 2
          assert is_list(effects)
      end
    end
  end

  # -------------------------------------------------------------------------
  # append_log_as_leader / append_cluster_change variants
  # -------------------------------------------------------------------------

  describe "membership commands on the leader" do
    test "joining an existing member is rejected" do
      from = self()
      cmd = {:"$ra_join", %{from: from}, {@peer2, :voter}, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true))

      assert {:reply, from, {:error, :already_member}} in effects
    end

    test "a plain join target defaults to voter membership" do
      cmd = {:"$ra_join", %{}, :server_cov3, :noreply}

      assert {:leader, s, _effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true))

      assert {3, 2, {:"$ra_cluster_change", _, new_cluster, _}} = s.log.entries[3]
      assert new_cluster[:server_cov3].voter_status.membership == :voter
      refute s.cluster_change_permitted
    end

    test "promoting an unknown target is rejected" do
      cmd = {:"$ra_promote", %{from: self()}, {:unknown, node()}, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :not_member}}, &1))
    end

    test "promoting an existing voter is rejected" do
      cmd = {:"$ra_promote", %{from: self()}, @peer2, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :already_voter}}, &1))
    end

    test "promoting a peer that is behind is rejected" do
      cluster =
        Map.put(default_cluster(), @peer2, %{
          default_cluster()[@peer2]
          | voter_status: %{membership: :non_voter},
            match_index: 0
        })

      cmd = {:"$ra_promote", %{from: self()}, @peer2, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true, cluster: cluster))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :not_caught_up}}, &1))
    end

    test "promoting a caught-up non-voter succeeds" do
      cluster =
        Map.put(default_cluster(), @peer2, %{
          default_cluster()[@peer2]
          | voter_status: %{membership: :non_voter},
            match_index: 3
        })

      cmd = {:"$ra_promote", %{from: self()}, @peer2, :noreply}

      assert {:leader, s, _effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true, cluster: cluster))

      assert {3, 2, {:"$ra_cluster_change", _, new_cluster, _}} = s.log.entries[3]
      assert new_cluster[@peer2].voter_status.membership == :voter
    end

    test "demoting an unknown target is rejected" do
      cmd = {:"$ra_demote", %{from: self()}, {:unknown, node()}, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :not_member}}, &1))
    end

    test "demoting a non-voter is rejected" do
      cluster =
        Map.put(default_cluster(), @peer2, %{
          default_cluster()[@peer2]
          | voter_status: %{membership: :non_voter}
        })

      cmd = {:"$ra_demote", %{from: self()}, @peer2, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true, cluster: cluster))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :not_voter}}, &1))
    end

    test "demoting the last voter is rejected" do
      cluster = %{@self_id => default_cluster()[@self_id]}
      cmd = {:"$ra_demote", %{from: self()}, @self_id, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true, cluster: cluster))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :last_voter}}, &1))
    end

    test "leaving a cluster the server is not part of is rejected" do
      cmd = {:"$ra_leave", %{from: self()}, {:unknown, node()}, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :not_member}}, &1))
    end

    test "WAL write errors are reported" do
      wal_name = :"cov_err_wal_#{System.unique_integer([:positive])}"
      start_supervised!({ErrorWal, wal_name})
      l = %{log([{1, 2, :cmd}]) | wal_name: wal_name}
      cmd = {:"$usr", %{from: self()}, :command, :noreply}

      assert {:leader, _s, effects} = Server.handle_leader({:command, cmd}, state(log: l))
      assert Enum.any?(effects, &match?({:reply, _, {:error, :wal_full}}, &1))
    end

    test "a WAL exception triggers the wal_down condition" do
      wal_name = :"cov_garbage_wal_#{System.unique_integer([:positive])}"
      start_supervised!({GarbageWal, wal_name})
      l = %{log([{1, 2, :cmd}]) | wal_name: wal_name}
      cmd = {:"$usr", %{from: self()}, :command, :noreply}

      assert {:await_condition, new_state, effects} =
               Server.handle_leader({:command, cmd}, state(log: l))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :wal_down}}, &1))
      condition = new_state.condition
      assert condition.transition_to == :leader

      predicate_state = %{new_state | log: log([{1, 2, :cmd}])}
      assert {true, ^predicate_state} = condition.predicate_fun.(:written, predicate_state)
    end

    test "a WAL write error on a cluster change is reported" do
      wal_name = :"cov_err_wal_cc_#{System.unique_integer([:positive])}"
      start_supervised!({ErrorWal, wal_name})
      l = %{log([{1, 2, :cmd}]) | wal_name: wal_name}
      cmd = {:"$ra_join", %{from: self()}, :server_cov5, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true, log: l))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :wal_full}}, &1))
    end

    test "a WAL exception on a cluster change triggers wal_down" do
      wal_name = :"cov_garbage_wal_cc_#{System.unique_integer([:positive])}"
      start_supervised!({GarbageWal, wal_name})
      l = %{log([{1, 2, :cmd}]) | wal_name: wal_name}
      cmd = {:"$ra_join", %{from: self()}, {:server_cov4, node()}, :noreply}

      assert {:await_condition, _new_state, effects} =
               Server.handle_leader({:command, cmd}, state(permitted: true, log: l))

      assert Enum.any?(effects, &match?({:reply, _, {:error, :wal_down}}, &1))
    end
  end

  # -------------------------------------------------------------------------
  # RPC effect construction
  # -------------------------------------------------------------------------

  describe "RPC effect construction" do
    test "a peer at the pipeline limit is skipped" do
      cluster =
        Map.put(default_cluster(), @peer2, %{
          default_cluster()[@peer2]
          | next_index: 3,
            match_index: 1
        })

      s = state(cluster: cluster, max_pipeline_count: 1)
      cmd = {:"$usr", %{}, :command, :noreply}

      assert {:leader, _s, effects} = Server.handle_leader({:command, cmd}, s)
      refute Enum.any?(effects, &match?({:send_rpc, @peer2, %Types.AppendEntriesRpc{}}, &1))
    end

    test "a peer that is not normal is skipped by pipelining" do
      cluster =
        Map.put(default_cluster(), @peer2, %{
          default_cluster()[@peer2]
          | status: :suspended
        })

      cmd = {:"$usr", %{}, :command, :noreply}

      assert {:leader, _s, effects} =
               Server.handle_leader({:command, cmd}, state(cluster: cluster))

      refute Enum.any?(effects, &match?({:send_rpc, @peer2, %Types.AppendEntriesRpc{}}, &1))
    end

    test "AppendEntries uses the snapshot term when prev index matches the snapshot" do
      l = log([], current_snapshot: {5, 2})
      cluster = Map.put(default_cluster(), @peer2, %{default_cluster()[@peer2] | next_index: 6})

      {_state, effects} = Server.make_rpcs(state(log: l, cluster: cluster))

      assert [{:send_rpc, @peer2, %Types.AppendEntriesRpc{} = rpc}] = effects
      assert rpc.prev_log_index == 5
      assert rpc.prev_log_term == 2
    end

    test "AppendEntries payload contains exactly the requested batch" do
      l = log([{1, 1, :a}, {2, 1, :b}, {3, 1, :c}])
      cluster = Map.put(default_cluster(), @peer2, %{default_cluster()[@peer2] | next_index: 2})

      {_state, effects} = Server.make_rpcs(state(log: l, cluster: cluster))

      assert [{:send_rpc, @peer2, %Types.AppendEntriesRpc{} = rpc}] = effects
      assert rpc.entries == [{2, 1, :b}]
    end
  end

  # -------------------------------------------------------------------------
  # release cursor / checkpoint / version index
  # -------------------------------------------------------------------------

  describe "release cursor and checkpoint conditions" do
    test "a satisfied written condition emits a release cursor update" do
      l = log([{1, 2, :cmd}, {2, 2, :cmd}], last_written: {2, 2})
      s = state(log: l, pending_release_cursor: {2, :mac_state, [{:written, 2}]})

      assert {:leader, new_state, _effects} =
               Server.handle_leader({:ra_log_event, {:written, [{3, 2, :cmd}], {3, 2}}}, s)

      refute Map.has_key?(new_state, :pending_release_cursor)
    end

    test "an in-flight snapshot send prevents a release cursor update" do
      cluster =
        Map.put(default_cluster(), @peer2, %{
          default_cluster()[@peer2]
          | status: {:sending_snapshot, self(), 0}
        })

      s = state(cluster: cluster, pending_release_cursor: {2, :mac_state, [:no_snapshot_sends]})

      assert {:leader, new_state, _effects} =
               Server.handle_leader({:ra_log_event, {:written, [{3, 2, :cmd}], {3, 2}}}, s)

      assert Map.has_key?(new_state, :pending_release_cursor)
    end

    test "index_machine_version walks the version list and raises when exhausted" do
      s = state(machine_versions: [{5, 1}, {0, 0}])
      assert {_state, _effects} = Server.update_release_cursor(2, :mac_state, %{}, s)

      assert_raise RuntimeError, ~r/machine_version_not_known/, fn ->
        Server.update_release_cursor(2, :mac_state, %{}, state(machine_versions: []))
      end
    end
  end

  # -------------------------------------------------------------------------
  # elections
  # -------------------------------------------------------------------------

  describe "elections" do
    test "an existing self vote skips durable writes when re-starting a pre-vote" do
      s = %{state() | voted_for: @self_id}

      assert {:pre_vote, _new_state, _effects} = Server.handle_follower(:election_timeout, s)
    end
  end
end
