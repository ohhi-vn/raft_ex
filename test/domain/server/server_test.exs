defmodule RaftEx.Domain.ServerTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.{Conn, Machine, Types}
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Infra.Persistence.{Log, LogMeta}

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defmodule HaltPlug do
    def init(opts), do: opts
    def call(conn, _opts), do: Conn.halt(Conn.put_resp(conn, :halted_reply))
  end

  defmodule AuxMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: conf

    @impl true
    def apply(_meta, cmd, state), do: {state, cmd}

    @impl true
    def handle_aux(_raft_state, _type, cmd, aux, state), do: {:reply, cmd, aux, state}

    @impl true
    def handle_aux(_raft_state, _type, _cmd, aux, _log, state), do: {:no_reply, aux, state}
  end

  defmodule NoReplyMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: conf

    @impl true
    def apply(_meta, cmd, state), do: {state, cmd}

    @impl true
    def handle_aux(_raft_state, _type, _cmd, aux, state), do: {:no_reply, aux, state}
  end

  defmodule NoReplyEffectsMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: conf

    @impl true
    def apply(_meta, cmd, state), do: {state, cmd}

    @impl true
    def handle_aux(_raft_state, _type, _cmd, aux, state),
      do: {:no_reply, aux, state, [{:aux, :done}]}
  end

  setup do
    dir = RaftEx.TestSupport.tmp_dir("server_test")
    meta_name = :"server_test_meta_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogMeta, %{name: :server_test, names: %{log_meta: meta_name}, data_dir: dir}}
    )

    Process.put(:server_test_meta_name, meta_name)
    :ok
  end

  defp config(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "server-test-uid",
      log_id: "server-test-log",
      metrics_key: :server_test_metrics,
      machine: {:machine, Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: Keyword.get(opts, :mac_mod, Machine.Simple),
      effective_handle_aux_fun: Keyword.get(opts, :aux_fun, nil),
      command_plugs: Keyword.get(opts, :command_plugs, []),
      rpc_plugs: Keyword.get(opts, :rpc_plugs, []),
      machine_versions: Keyword.get(opts, :machine_versions, [{0, 0}]),
      system_config: %{names: %{log_meta: Process.get(:server_test_meta_name)}}
    )
  end

  defp log(entries \\ []) do
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
      range: range,
      last_term: last_term,
      next_index: last_index + 1,
      last_written_index_term: {last_index, last_term},
      meta_name: Process.get(:server_test_meta_name),
      cfg: %{uid: "server-test-uid"}
    }
  end

  defp self_state(opts \\ []), do: state(opts)

  defp state(opts \\ []) do
    %{
      cfg: config(opts),
      current_term: 2,
      commit_index: 1,
      last_applied: 1,
      persisted_last_applied: 3,
      leader_id: nil,
      voted_for: nil,
      commit_latency: 0,
      rpc_from: nil,
      membership: :voter,
      cluster_change_permitted: false,
      pending_consistent_queries: [],
      cluster: %{
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
      },
      log: Keyword.get(opts, :log, log([{1, 2, :cmd}, {2, 2, :cmd}])),
      machine_state: :machine_state,
      aux_state: Keyword.get(opts, :aux_state, nil)
    }
  end

  describe "accessors" do
    test "id, uid, log_id, system_config, current_term, machine_version, machine" do
      s = state()

      assert Server.id(s) == @self_id
      assert Server.uid(s) == "server-test-uid"
      assert Server.log_id(s) == "server-test-log"
      assert is_map(Server.system_config(s))
      assert Map.has_key?(Server.system_config(s), :names)
      assert Server.current_term(s) == 2
      assert Server.machine_version(s) == 0
      assert Server.machine(s) == {:machine, Machine.Simple, %{}}
    end

    test "leader_id and clear_leader_id" do
      s = %{state() | leader_id: @peer2}
      assert Server.leader_id(s) == @peer2
      refute Server.leader_id(state())
      refute Server.leader_id(Server.clear_leader_id(s))
    end
  end

  describe "log predicates" do
    test "is_new?/1" do
      assert Server.is_new?(state(log: %Log{next_index: 1}))
      refute Server.is_new?(state(log: %Log{next_index: 4}))
    end

    test "is_fully_persisted?/1" do
      s = state(log: log([{1, 2, :cmd}]))
      assert Server.is_fully_persisted?(s)

      s = %{s | log: %{s.log | last_written_index_term: {0, 0}}}
      refute Server.is_fully_persisted?(s)
    end
  end

  describe "membership helpers" do
    test "get_membership/1 uses the state's default" do
      assert Server.get_membership(state()) == :voter
      assert Server.get_membership(%{state() | membership: :promotable}) == :promotable
    end

    test "get_membership/2 accepts an explicit cluster" do
      cluster = %{@self_id => %{voter_status: %{uid: "server-test-uid", membership: :non_voter}}}
      assert Server.get_membership(cluster, state()) == :non_voter
    end

    test "peers/1 excludes self" do
      assert Map.keys(Server.peers(state())) == [@peer2]
    end

    test "update_peer/3 merges peer state" do
      s = Server.update_peer(@peer2, %{match_index: 5}, state())
      assert s.cluster[@peer2].match_index == 5
      assert s.cluster[@peer2].next_index == 1
    end
  end

  describe "state_query/2" do
    test "returns the requested projection" do
      s = state()

      assert Server.state_query(:members, s) == [@self_id, @peer2]
      assert Server.state_query(:members_info, s) == s.cluster
      assert Server.state_query(:leader, s) == nil
      assert Server.state_query(:machine, s) == :machine_state
      assert Server.state_query(:last_applied, s) == 1
      assert Server.state_query(:initial_members, s) == [@self_id, @peer2]
      assert is_map(Server.state_query(:overview, s))
      assert Server.state_query(:unknown, s) == s
    end

    test "overview/1 contains the key consensus fields" do
      overview = Server.overview(state())

      assert overview.current_term == 2
      assert overview.commit_index == 1
      assert overview.last_applied == 1
      assert is_map(overview.cluster)
      assert Map.has_key?(overview, :leader_id)
      assert Map.has_key?(overview, :voted_for)
      assert Map.has_key?(overview, :membership)
    end
  end

  describe "machine_query/2" do
    test "runs the query against the machine state with context" do
      fun = fn s -> s + 1 end
      mac_state = {:simple, fun, 5}
      s = %{state() | machine_state: mac_state}

      assert {{1, 2}, 5} = Server.machine_query(fn value -> value end, s)
    end
  end

  describe "persist_last_applied/1" do
    test "leaves state unchanged when last_applied has not advanced" do
      s = state()
      assert Server.persist_last_applied(s) == s
    end
  end

  describe "fetch_term/2 and transform_for_partial_read/3" do
    test "fetch_term returns the term of a log entry" do
      assert {2, _state} = Server.fetch_term(1, state())
    end

    test "fetch_term falls back to the snapshot" do
      s = state(log: %Log{current_snapshot: {3, 4}, entries: %{}})
      assert {4, _state} = Server.fetch_term(3, s)
      assert {nil, _state} = Server.fetch_term(2, s)
    end

    test "transform_for_partial_read unwraps user commands" do
      assert Server.transform_for_partial_read(1, 2, {:"$usr", %{}, :command, :noreply}) ==
               :command

      assert Server.transform_for_partial_read(1, 2, :other) == :other
    end
  end

  describe "state-enter, aux, tick" do
    test "handle_state_enter changes role and delegates to the machine" do
      {state, effects} = Server.handle_state_enter(:leader, state())
      assert is_map(state)
      assert effects == []
    end

    test "handle_aux without a handler replies with an error for calls" do
      {role, state, effects} = Server.handle_aux(:leader, :call, :cmd, state())
      assert role == :leader
      assert state.current_term == 2
      assert effects == [{:reply, {:error, :aux_handler_not_implemented}}]
    end

    test "handle_aux without a handler is silent for casts" do
      {_role, _state, effects} = Server.handle_aux(:leader, :cast, :cmd, state())
      assert effects == []
    end

    test "tick delegates to the machine" do
      assert Server.tick(state()) == []
    end

    test "log_tick updates the log's last resend time" do
      state = Server.log_tick(state())
      assert is_integer(state.log.last_resend_time)
    end
  end

  describe "message handlers" do
    test "handle_leader ignores unknown messages" do
      s = state()
      assert {:leader, ^s, []} = Server.handle_leader(:unknown, s)
    end

    test "handle_follower ignores unknown messages" do
      s = state()
      assert {:follower, ^s, []} = Server.handle_follower(:unknown, s)
    end

    test "handle_follower sends a reply for a halted RPC" do
      s = state(rpc_plugs: [{HaltPlug, []}])
      rpc = %Types.AppendEntriesRpc{term: 2, leader_id: @peer2}

      assert {:follower, _state, [{:send_rpc, @peer2, :halted_reply}]} =
               Server.handle_follower(rpc, s)
    end
  end

  describe "leader command handling" do
    test "appends a user command and builds replication effects" do
      cmd = {:"$usr", %{}, :command, :noreply}
      assert {:leader, state, effects} = Server.handle_leader({:command, cmd}, self_state())

      assert state.log.entries[3] == {3, 2, cmd}
      assert Enum.any?(effects, &match?({:send_rpc, _, %Types.AppendEntriesRpc{}}, &1))
    end

    test "a halted command plug produces a reply and does not append" do
      s = self_state(command_plugs: [{HaltPlug, []}])
      from = self()
      cmd = {:"$usr", %{from: from}, :command, :await_consensus}

      assert {:leader, state, effects} = Server.handle_leader({:command, cmd}, s)
      assert state.log.entries == s.log.entries
      assert effects == [{:reply, from, {:wrap_reply, :halted_reply}}]
    end

    test "cluster changes are rejected when not permitted" do
      cmd = {:"$ra_join", self(), @peer2, :noreply}
      assert {:leader, state, effects} = Server.handle_leader({:command, cmd}, self_state())

      assert state.log.entries == self_state().log.entries
      assert effects == []
    end

    test "handles written log events" do
      evt = {:written, [{3, 2, :cmd}], {3, 2}}
      assert {:leader, _state, effects} = Server.handle_leader({:ra_log_event, evt}, self_state())
      assert Enum.any?(effects, &match?({:next_event, :info, :pipeline_rpcs}, &1))
    end

    test "handles generic log events" do
      evt = {:written, [{3, 2, :cmd}], 3}

      assert {:leader, _state, _effects} =
               Server.handle_leader({:ra_log_event, evt}, self_state())
    end

    test "applies committed user entries to the machine" do
      cmd = {:"$usr", %{}, :do_something, :noreply}
      log = log([{1, 2, cmd}, {2, 2, cmd}])
      s = %{self_state(log: log) | commit_index: 2, last_applied: 0}
      s = %{s | machine_state: {:simple, fn _cmd, st -> st end, 0}}

      evt = {:written, [{1, 2, cmd}, {2, 2, cmd}], {2, 2}}
      assert {:leader, state, _effects} = Server.handle_leader({:ra_log_event, evt}, s)
      assert state.last_applied == 2
    end

    test "applies committed cluster-change entries" do
      cmd = {:"$ra_cluster_change", %{}, %{@self_id => %{}}, :noreply}
      log = log([{1, 2, cmd}])
      s = %{self_state(log: log) | commit_index: 1, last_applied: 0}

      evt = {:written, [{1, 2, cmd}], {1, 2}}
      assert {:leader, state, _effects} = Server.handle_leader({:ra_log_event, evt}, s)
      assert state.last_applied == 1
    end

    test "forwards append entries replies from known peers" do
      reply = %Types.AppendEntriesReply{term: 2, success: false, next_index: 1}
      s = %{self_state() | rpc_from: @peer2}

      assert {:leader, _state, effects} = Server.handle_leader(reply, s)
      assert effects == [{:send_append_entries, @peer2}]
    end
  end

  describe "elections, aux handlers and cursor helpers" do
    test "election timeout starts a pre-vote" do
      assert {:pre_vote, state, effects} = Server.handle_follower(:election_timeout, self_state())

      assert is_reference(state.pre_vote_token)
      assert state.votes == 0

      assert {:send_vote_requests, [{@peer2, %Types.PreVoteRpc{}}]} =
               Enum.find(effects, &match?({:send_vote_requests, _}, &1))
    end

    test "handle_aux no-reply variants" do
      s = self_state(aux_fun: {:handle_aux, 5}, mac_mod: NoReplyMachine, aux_state: :old)

      assert {:leader, _state, []} = Server.handle_aux(:leader, :cast, :cmd, s)

      s = %{s | cfg: %{s.cfg | effective_machine_module: NoReplyEffectsMachine}}

      assert {:leader, _state, [{:aux, :done}]} = Server.handle_aux(:leader, :cast, :cmd, s)
    end

    test "terminate closes the log" do
      assert :ok = Server.terminate(self_state(), :normal)
    end

    test "make_rpcs can request a snapshot" do
      log = %Log{current_snapshot: {5, 2}, entries: %{}, range: nil, meta_name: :meta}
      s = self_state(log: log)

      {_state, effects} = Server.make_rpcs(s)
      assert Enum.any?(effects, &match?({:send_snapshot, _, _}, &1))
    end

    test "handle_aux with a 5-arity machine handler" do
      s = self_state(aux_fun: {:handle_aux, 5}, mac_mod: AuxMachine, aux_state: :aux)

      assert {:leader, state, [{:reply, :cmd}]} = Server.handle_aux(:leader, :call, :cmd, s)
      assert state.aux_state == :aux
    end

    test "update_release_cursor, promote_checkpoint and checkpoint" do
      s = self_state()

      {state, effects} = Server.update_release_cursor(2, :mac_state, %{}, s)
      assert is_map(state)
      assert is_list(effects)

      {state, effects} = Server.promote_checkpoint(2, s)
      assert is_map(state)
      assert is_list(effects)

      {state, effects} = Server.checkpoint(2, :mac_state, s)
      assert is_map(state)
      assert is_list(effects)
    end

    test "handle_follower delegates a non-halted RPC" do
      rpc = %Types.AppendEntriesRpc{term: 1, leader_id: @peer2}
      assert {:follower, _state, effects} = Server.handle_follower(rpc, self_state())
      assert Enum.any?(effects, &match?({:send_rpc, @peer2, %Types.AppendEntriesReply{}}, &1))
    end
  end

  describe "make_rpcs/1" do
    test "builds one AppendEntries RPC per peer" do
      {state, effects} = Server.make_rpcs(state())

      assert is_map(state)
      assert length(effects) == 1
      assert [{:send_rpc, @peer2, %Types.AppendEntriesRpc{} = rpc}] = effects
      assert rpc.term == 2
      assert rpc.leader_id == @self_id
    end
  end
end
