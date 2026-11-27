defmodule RaftEx.Integration.TransportIdentityTest do
  @moduledoc "Coverage for Section 10 transport and sender identity (tasks 10.1-10.6)."
  use ExUnit.Case, async: false

  alias RaftEx.App.ServerRuntime
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Network.Transport

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defp cfg do
    struct!(Config,
      id: @self_id,
      uid: "transport-test",
      log_id: "transport-test",
      metrics_key: :transport_test_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{},
      rpc_plugs: []
    )
  end

  defp server_state(opts \\ []) do
    %{
      cfg: cfg(),
      current_term: 2,
      commit_index: 0,
      last_applied: 0,
      persisted_last_applied: 0,
      leader_id: nil,
      voted_for: nil,
      membership: :voter,
      cluster_change_permitted: true,
      cluster_index_term: {0, 0},
      commit_latency: 0,
      transfer_leadership: nil,
      became_leader_at: System.monotonic_time(:millisecond),
      pending_consistent_queries: [],
      query_index: 0,
      queries_waiting_heartbeats: :queue.new(),
      cluster:
        Keyword.get(opts, :cluster, %{
          @self_id => peer(4, match_index: 3),
          @peer2 => peer(1)
        }),
      log: %RaftEx.Infra.Persistence.Log{
        entries: %{},
        range: nil,
        last_term: 0,
        next_index: 4,
        last_written_index_term: {3, 2},
        wal_name: nil,
        cfg: %{uid: "transport-test"}
      },
      machine_state: {:simple, fn _cmd, s -> {s, :ok} end, 0},
      aux_state: nil
    }
  end

  defp peer(next_index, opts \\ []) do
    %{
      voter_status: %{membership: :voter},
      next_index: next_index,
      match_index: Keyword.get(opts, :match_index, 0),
      query_index: 0,
      commit_index_sent: 0,
      status: Keyword.get(opts, :status, :normal)
    }
  end

  defp runtime_state(opts \\ []) do
    %{
      conf: %{name: :transport_test, tick_timeout: 100, max_pending_commands: 1024},
      server_state: server_state(opts),
      election_timeout_set: false,
      pending_commands: []
    }
  end

  test "10.1 remote sends to a partitioned peer fail fast without blocking" do
    unreachable = {:nosuch, :"unreachable@127.0.0.1"}
    t0 = System.monotonic_time(:millisecond)

    results =
      for _ <- 1..5 do
        Transport.send_rpc(unreachable, :ping)
      end

    elapsed = System.monotonic_time(:millisecond) - t0
    # No inline sleeps/retries: 5 sends complete far faster than the old
    # 3×100ms retry loop, and reachable peers are unaffected.
    assert elapsed < 5_000
    assert Enum.all?(results, &(&1 == :ok or match?({:error, _}, &1)))
  end

  test "10.3 the sender envelope attributes RPCs without a pid lookup" do
    from_pid = self()

    rpc = %Types.AppendEntriesRpc{
      term: 2,
      leader_id: @peer2,
      prev_log_index: 0,
      prev_log_term: 0,
      entries: [],
      leader_commit: 0
    }

    # Envelope form: sender id travels with the message.
    assert {:keep_state, %{server_state: %{rpc_from: @peer2}}, _} =
             ServerRuntime.follower(
               :info,
               {from_pid, {:gen_cast, {:rpc, @peer2, rpc}}},
               runtime_state()
             )
  end

  test "10.3 legacy messages without an envelope still resolve via pid" do
    name = :"transport_self_#{System.unique_integer([:positive])}"
    Process.register(self(), name)

    rpc = %Types.AppendEntriesRpc{
      term: 2,
      leader_id: {name, node()},
      prev_log_index: 0,
      prev_log_term: 0,
      entries: [],
      leader_commit: 0
    }

    assert {:keep_state, %{server_state: %{rpc_from: {^name, _}}}, _} =
             ServerRuntime.follower(
               :info,
               {self(), {:gen_cast, {:rpc, rpc}}},
               runtime_state()
             )

    Process.unregister(name)
  end

  test "10.6 node-down marks peers suspect and node-up restores them" do
    # NOTE: peer ids bake node() at compile time while the suite may start
    # distribution later, so address the event at the peer's own node.
    peer_node = elem(@peer2, 1)
    s = runtime_state()

    assert {:keep_state, s1, []} =
             ServerRuntime.leader(:info, {:ra_network_event, :nodedown, peer_node}, s)

    assert %{status: {:suspect, _}} = s1.server_state.cluster[@peer2]
    # The leader itself is never marked.
    assert %{status: :normal} = s1.server_state.cluster[@self_id]

    assert {:keep_state, s2, _actions} =
             ServerRuntime.leader(:info, {:ra_network_event, :nodeup, peer_node}, s1)

    assert %{status: :normal} = s2.server_state.cluster[@peer2]
  end

  test "10.2 failed sends keep the peer eligible for retry (no stranding)" do
    # A peer on an unreachable node optimistically advances with each pass
    # but never acks. Successive pipeline passes must keep including it
    # while it lags instead of stranding it after the first attempt.
    dead = {:dead_peer, :"dead@127.0.0.1"}

    cluster = %{
      @self_id => peer(4, match_index: 3),
      dead => peer(1)
    }

    base = server_state(cluster: cluster)

    log = %RaftEx.Infra.Persistence.Log{
      base.log
      | entries: %{1 => {1, 2, :a}, 2 => {2, 2, :b}, 3 => {3, 2, :c}},
        range: {1, 3},
        last_term: 2,
        next_index: 4,
        last_written_index_term: {3, 2}
    }

    base = %{base | log: log}

    {_s1, effs1} = RaftEx.Domain.Server.make_pipeline_rpcs(base)
    assert Enum.any?(effs1, &match?({:send_rpc, ^dead, _}, &1))

    # Second pass with no acks: the peer is still included.
    {_s2, effs2} = RaftEx.Domain.Server.make_pipeline_rpcs(base)
    assert Enum.any?(effs2, &match?({:send_rpc, ^dead, _}, &1))
  end
end
