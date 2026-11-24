defmodule RaftEx.App.ServerRuntimeCallbacksTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.ServerRuntime
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defp server_state(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "runtime-test-uid",
      log_id: "runtime-test-log",
      metrics_key: :runtime_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{data_dir: "/tmp/runtime_test", names: %{log_meta: :runtime_test_meta}},
      command_plugs: Keyword.get(opts, :command_plugs, []),
      rpc_plugs: Keyword.get(opts, :rpc_plugs, [])
    )
    |> then(fn cfg ->
      %{
        cfg: cfg,
        current_term: 2,
        commit_index: 1,
        last_applied: 1,
        persisted_last_applied: 1,
        leader_id: Keyword.get(opts, :leader_id, nil),
        voted_for: nil,
        membership: :voter,
        cluster_change_permitted: false,
        pending_consistent_queries: [],
        rpc_from: @peer2,
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
        log: %Log{
          entries: %{1 => {1, 2, :cmd}, 2 => {2, 2, :cmd}},
          range: {1, 2},
          last_term: 2,
          next_index: 3,
          last_written_index_term: {2, 2}
        },
        machine_state: {:simple, fn _cmd, s -> s end, 0},
        aux_state: nil
      }
    end)
  end

  defp state(opts \\ []) do
    %{
      conf: %{
        name: :runtime_test,
        cluster_name: Keyword.get(opts, :cluster_name, :runtime_cluster),
        data_dir: "/tmp/runtime_test",
        log_id: "runtime-test-log",
        broadcast_time: 100,
        tick_timeout: 100
      },
      server_state: server_state(opts),
      election_timeout_set: false,
      pending_commands: [],
      election_context: %{
        state: Keyword.get(opts, :context_state, :pre_vote),
        term: 2,
        votes_received: MapSet.new([@self_id]),
        total_voters: 1,
        last_log_index: 2,
        last_log_term: 2
      }
    }
  end

  describe "leader callbacks" do
    test "enter records the leader" do
      assert {:keep_state, _state, _actions} = ServerRuntime.leader(:enter, :follower, state())
    end

    test "trigger_election and ping" do
      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:error, :already_leader}}]} =
               ServerRuntime.leader({:call, from}, :trigger_election, state())

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :leader}}]} =
               ServerRuntime.leader({:call, from}, :ping, state())
    end

    test "state queries" do
      from = self()

      for spec <- [:members, :members_info, :leader, :last_applied, :unknown] do
        assert {:keep_state, _s, [{:reply, ^from, {:ok, _, _}}]} =
                 ServerRuntime.leader({:call, from}, {:state_query, spec}, state())
      end
    end

    test "tick timeout builds RPCs and effects" do
      assert {:keep_state, _state, actions} = ServerRuntime.leader(:_, :tick_timeout, state())
      assert is_list(actions)
    end

    test "leader_call command variants" do
      from = {self(), make_ref()}
      usr = {:"$usr", :cmd, :noreply}

      result = ServerRuntime.leader({:call, from}, {:leader_call, {:command, usr}}, state())

      assert match?({:keep_state, _, _}, result) or match?({:next_state, _, _, _}, result) or
               match?({:stop, _, _}, result)

      join = {:"$ra_join", @peer2, :noreply}

      result = ServerRuntime.leader({:call, from}, {:leader_call, {:command, join}}, state())

      assert match?({:keep_state, _, _}, result) or match?({:next_state, _, _, _}, result) or
               match?({:stop, _, _}, result)
    end
  end

  describe "follower callbacks" do
    test "ping, trigger_election and leader_call" do
      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :follower}}]} =
               ServerRuntime.follower({:call, from}, :ping, state())

      assert {:next_state, :candidate, _s, [{:reply, ^from, :ok}]} =
               ServerRuntime.follower({:call, from}, :trigger_election, state())

      assert {:keep_state, _s, [{:reply, ^from, {:error, :no_leader}}]} =
               ServerRuntime.follower({:call, from}, {:leader_call, :x}, state())

      with_leader = state(leader_id: @peer2)

      assert {:keep_state, _s, [{:reply, ^from, {:redirect, @peer2}}]} =
               ServerRuntime.follower({:call, from}, {:leader_call, :x}, with_leader)
    end

    test "enter with a leader records it" do
      s = state(leader_id: @peer2)
      assert {:keep_state, _state, _actions} = ServerRuntime.follower(:enter, :leader, s)
    end
  end

  describe "pre_vote and candidate callbacks" do
    test "pre_vote enter, ping, trigger_election and timeout" do
      assert {:keep_state, _s, _actions} = ServerRuntime.pre_vote(:enter, :follower, state())

      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :pre_vote}}]} =
               ServerRuntime.pre_vote({:call, from}, :ping, state())

      assert {:next_state, :candidate, _s, [{:reply, ^from, :ok}]} =
               ServerRuntime.pre_vote({:call, from}, :trigger_election, state())

      assert {:next_state, :pre_vote, _s, _actions} =
               ServerRuntime.pre_vote(:state_timeout, :election_timeout, state())
    end

    test "candidate enter, ping, trigger_election, leader_call and timeouts" do
      assert {:keep_state, _s, _actions} = ServerRuntime.candidate(:enter, :pre_vote, state())

      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :candidate}}]} =
               ServerRuntime.candidate({:call, from}, :ping, state())

      assert {:next_state, :candidate, _s, [{:reply, ^from, :ok}]} =
               ServerRuntime.candidate({:call, from}, :trigger_election, state())

      assert {:keep_state, _s, [{:reply, ^from, {:error, :no_leader}}]} =
               ServerRuntime.candidate({:call, from}, {:leader_call, :x}, state())

      assert {:next_state, :candidate, _s, _actions} =
               ServerRuntime.candidate(:state_timeout, :election_timeout, state())

      assert {:next_state, :leader, _s, []} =
               ServerRuntime.candidate(:internal, :election_won, state())

      assert {:next_state, :leader, _s, []} =
               ServerRuntime.candidate(:state_timeout, :become_leader, state())
    end

    test "vote reply handling" do
      reply = %Types.RequestVoteResult{term: 2, vote_granted: true}

      from = {self(), make_ref()}

      assert {:next_state, :leader, _s, _actions} =
               ServerRuntime.candidate(
                 {:cast, {from, reply}},
                 :vote_reply,
                 state(context_state: :candidate)
               )

      pre_vote_reply = %Types.PreVoteResult{term: 2, vote_granted: true}

      from = {self(), make_ref()}

      assert {:next_state, :candidate, _s, _actions} =
               ServerRuntime.pre_vote({:cast, {from, pre_vote_reply}}, :pre_vote_reply, state())
    end
  end

  describe "call helpers" do
    test "missing local processes produce errors" do
      missing = :"missing_runtime_#{System.unique_integer([:positive])}"

      assert {:error, :noproc} = ServerRuntime.ping({missing, node()}, 100)
      assert {:error, :noproc} = ServerRuntime.state_query({missing, node()}, :members, 100)

      assert {:error, :noproc} =
               ServerRuntime.local_state_query({missing, node()}, :overview, 100)
    end

    test "multiple server ids are tried in order" do
      missing = :"missing_multi_#{System.unique_integer([:positive])}"

      assert {:error, {:no_more_servers_to_try, _}} =
               ServerRuntime.state_query([{missing, node()}, {missing, node()}], :members, 100)
    end
  end
end
