defmodule RaftEx.App.ServerRuntimeLivenessTest do
  @moduledoc "Unit coverage for Section 5 runtime liveness: await_condition (5.1), unsupported-call replies (5.2), local queries in every state (5.8), and RPC plug coverage (5.9)."
  use ExUnit.Case, async: false

  alias RaftEx.App.ServerRuntime
  alias RaftEx.Domain.Conn
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types

  defmodule HaltAll do
    def init(opts), do: opts
    def call(conn, _opts), do: conn |> Conn.put_resp(:halted) |> Conn.halt()
  end

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defp cfg(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "liveness-uid",
      log_id: "liveness-log",
      metrics_key: :liveness_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{},
      rpc_plugs: Keyword.get(opts, :rpc_plugs, [])
    )
  end

  defp server_state(opts \\ []) do
    %{
      cfg: cfg(opts),
      current_term: 2,
      commit_index: 1,
      last_applied: 1,
      persisted_last_applied: 1,
      leader_id: Keyword.get(opts, :leader_id, nil),
      voted_for: nil,
      membership: :voter,
      cluster_change_permitted: true,
      pending_consistent_queries: [],
      cluster: %{
        @self_id => %{voter_status: %{membership: :voter}, next_index: 1, match_index: 0}
      },
      log: %RaftEx.Infra.Persistence.Log{
        entries: %{},
        range: nil,
        last_term: 0,
        next_index: 1,
        wal_name: nil,
        cfg: %{uid: "liveness-uid"}
      },
      machine_state: {:simple, nil, :qstate},
      aux_state: nil
    }
  end

  defp runtime_state(opts \\ []) do
    %{
      conf: %{
        name: :liveness_test,
        tick_timeout: 100,
        await_condition_timeout: Keyword.get(opts, :await_timeout, 60_000)
      },
      server_state: server_state(opts),
      election_timeout_set: false
    }
  end

  describe "await_condition (5.1)" do
    test "entering arms a state timeout and polling a recovered condition resumes" do
      cond_map = %{predicate_fun: fn _msg, ss -> {true, ss} end, transition_to: :leader}
      base = runtime_state()
      s = %{base | server_state: Map.put(base.server_state, :condition, cond_map)}

      assert {:keep_state, s1, [{:state_timeout, _, :await_condition_timeout}]} =
               ServerRuntime.await_condition(:enter, :old, s)

      assert {:next_state, :leader, s2, _} =
               ServerRuntime.await_condition(:state_timeout, :await_condition_timeout, s1)

      refute Map.has_key?(s2, :await_deadline)
    end

    test "a still-failing condition re-arms instead of wedging" do
      cond_map = %{predicate_fun: fn _msg, ss -> {false, ss} end, transition_to: :leader}
      base = runtime_state(await_timeout: 60_000)
      s = %{base | server_state: Map.put(base.server_state, :condition, cond_map)}

      assert {:keep_state, s1, [{:state_timeout, _, :await_condition_timeout}]} =
               ServerRuntime.await_condition(:enter, :old, s)

      assert {:keep_state, _, [{:state_timeout, _, :await_condition_timeout}]} =
               ServerRuntime.await_condition(:state_timeout, :await_condition_timeout, s1)
    end

    test "an expired deadline steps down to follower" do
      cond_map = %{predicate_fun: fn _msg, ss -> {false, ss} end, transition_to: :leader}
      base = runtime_state(await_timeout: 1)
      s = %{base | server_state: Map.put(base.server_state, :condition, cond_map)}
      {:keep_state, s1, _} = ServerRuntime.await_condition(:enter, :old, s)
      Process.sleep(5)

      assert {:next_state, :follower, s2, []} =
               ServerRuntime.await_condition(:state_timeout, :await_condition_timeout, s1)

      refute Map.has_key?(s2.server_state, :condition)
    end

    test "calls arriving while blocked get an error instead of hanging" do
      cond_map = %{predicate_fun: fn _msg, ss -> {false, ss} end, transition_to: :leader}
      base = runtime_state()
      s = %{base | server_state: Map.put(base.server_state, :condition, cond_map)}
      from = {self(), make_ref()}

      assert {:keep_state, _s, [{:reply, ^from, {:error, :wal_down}}]} =
               ServerRuntime.await_condition({:call, from}, :anything, s)
    end
  end

  describe "unsupported calls (5.2)" do
    test "an unknown call gets an error reply instead of hanging" do
      from = {self(), make_ref()}

      assert {:keep_state, _s, [{:reply, ^from, {:error, :unsupported}}]} =
               ServerRuntime.leader({:call, from}, :bogus_message, runtime_state())

      assert {:keep_state, _s, [{:reply, ^from, {:error, :unsupported}}]} =
               ServerRuntime.follower({:call, from}, :bogus_message, runtime_state())
    end

    test "a consistent query is not answered with a spurious error" do
      from = {self(), make_ref()}

      assert {:keep_state, _s, actions} =
               ServerRuntime.leader(
                 {:call, from},
                 {:leader_call, {:consistent_query, fn _ -> :ok end}},
                 runtime_state()
               )

      refute Enum.any?(actions, &match?({:reply, _, {:error, :unsupported}}, &1))
    end
  end

  describe "local queries in every state (5.8)" do
    test "member overview is answered directly without redirect" do
      from = {self(), make_ref()}

      for state_fun <- [:leader, :follower, :candidate, :pre_vote] do
        result =
          apply(ServerRuntime, state_fun, [
            {:call, from},
            {:local_call, {:state_query, :members}},
            runtime_state()
          ])

        assert {:keep_state, _s, [{:reply, ^from, {:ok, [_ | _], _}}]} = result
      end
    end

    test "unknown local queries fail fast" do
      from = {self(), make_ref()}

      assert {:keep_state, _s, [{:reply, ^from, {:error, :unsupported}}]} =
               ServerRuntime.follower({:call, from}, {:local_call, :bogus}, runtime_state())
    end
  end

  describe "RPC plug pipeline (5.9)" do
    defp halting_state do
      base = runtime_state()
      %{base | server_state: %{base.server_state | cfg: cfg(rpc_plugs: [{HaltAll, []}])}}
    end

    test "a halting plug stops a pre-vote request" do
      rpc = %Types.PreVoteRpc{
        term: 2,
        candidate_id: @peer2,
        token: make_ref(),
        machine_version: 0,
        last_log_index: 0,
        last_log_term: 0
      }

      {_next, _state, effects} =
        RaftEx.Domain.Server.handle_follower(rpc, halting_state().server_state)

      assert [{:send_rpc, @peer2, %Types.PreVoteResult{vote_granted: false}}] = effects
    end

    test "a halting plug stops a vote request" do
      rpc = %Types.RequestVoteRpc{
        term: 2,
        candidate_id: @peer2,
        last_log_index: 0,
        last_log_term: 0
      }

      {_next, _state, effects} =
        RaftEx.Domain.Server.handle_follower(rpc, halting_state().server_state)

      assert [{:send_rpc, @peer2, %Types.RequestVoteResult{vote_granted: false}}] = effects
    end

    test "a halting plug stops heartbeats and snapshot transfers" do
      hb = %Types.HeartbeatRpc{term: 2, leader_id: @peer2, query_index: 0, commit_index: 0}

      {_next, _state, hb_effects} =
        RaftEx.Domain.Server.handle_follower(hb, halting_state().server_state)

      assert [{:send_rpc, @peer2, %Types.HeartbeatReply{}}] = hb_effects

      snap = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @peer2,
        meta: %{},
        chunk_state: :full,
        data: :x
      }

      {_next, _state, snap_effects} =
        RaftEx.Domain.Server.handle_follower(snap, halting_state().server_state)

      assert [{:send_rpc, @peer2, %Types.InstallSnapshotResult{}}] = snap_effects
    end

    test "without plugs every RPC type reaches protocol handling" do
      st = runtime_state().server_state

      pre = %Types.PreVoteRpc{
        term: 1,
        candidate_id: @peer2,
        token: make_ref(),
        machine_version: 0,
        last_log_index: 0,
        last_log_term: 0
      }

      assert {_g, _} = RaftEx.Domain.Server.Election.handle_pre_vote_request(pre, st)

      vote = %Types.RequestVoteRpc{
        term: 1,
        candidate_id: @peer2,
        last_log_index: 0,
        last_log_term: 0
      }

      assert {false, _, _} = RaftEx.Domain.Server.Election.handle_vote_request(vote, st)

      hb = %Types.HeartbeatRpc{term: 1, leader_id: @peer2, query_index: 0, commit_index: 0}
      assert {_state, _} = RaftEx.Domain.Server.RpcHandler.handle_heartbeat_rpc(hb, st)
    end
  end
end
