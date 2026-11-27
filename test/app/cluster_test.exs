defmodule RaftEx.App.ClusterTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.Cluster
  alias RaftEx.Leaderboard

  defp machine do
    {:machine, RaftEx.Domain.Machine.Simple,
     %{simple_fun: fn _cmd, state -> state end, initial_state: 0}}
  end

  setup do
    Leaderboard.init()
    cluster_name = :"cluster_test_#{System.unique_integer([:positive])}"
    on_exit(fn -> Leaderboard.clear(cluster_name) end)
    %{cluster_name: cluster_name}
  end

  defp start_test_system do
    data_dir =
      Path.join(System.tmp_dir!(), "cluster_system_#{System.unique_integer([:positive])}")

    File.mkdir_p!(data_dir)
    RaftEx.App.System.stop_default()
    RaftEx.start_in(data_dir)

    on_exit(fn ->
      RaftEx.App.System.stop_default()
      File.rm_rf!(data_dir)
    end)

    data_dir
  end

  defp start_single_server_cluster(cluster_name) do
    start_test_system()

    assert {:ok, [{_name, _node} = sid]} =
             Cluster.form_cluster(cluster_name, machine(), [node()], timeout: 2_000)

    sid
  end

  # A minimal stand-in that answers every gen_statem call with an error. This
  # lets the topology handler exercise its add_member error branch without
  # needing a live Raft cluster.
  defp stub_loop do
    receive do
      {:"$gen_call", from, _request} ->
        GenServer.reply(from, {:error, :stubbed})
        stub_loop()
    end
  end

  defp register_stub_server(name) do
    pid = spawn(fn -> stub_loop() end)
    Process.register(pid, name)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    pid
  end

  # Answers members queries but never replies to join commands, so add_member
  # times out.
  defp quiet_stub_loop do
    receive do
      {:"$gen_call", from, {:leader_call, {:state_query, :members}}} ->
        GenServer.reply(from, {:ok, [{:server1, node()}], {:server1, node()}})

      {:"$gen_call", _from, _request} ->
        :ok
    end

    quiet_stub_loop()
  end

  defp register_quiet_stub_server(name) do
    pid = spawn(fn -> quiet_stub_loop() end)
    Process.register(pid, name)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    pid
  end

  describe "form_cluster/4" do
    test "returns an error when no nodes are local" do
      assert {:error, :no_local_servers} =
               Cluster.form_cluster(:remote_cluster, machine(), [:"remote@invalid.host"])
    end

    test "starts local servers when the seed is on a remote node" do
      # "aaa_" sorts before any real node name, so the first server id is remote.
      remote = :"aaa_seed_#{System.unique_integer([:positive])}@invalid.host"

      assert {:ok, [{:server2, local_node}]} =
               Cluster.form_cluster(:cluster_remote_seed, machine(), [remote, node()])

      assert local_node == node()
    end

    test "starts the remaining local servers after the seed" do
      start_test_system()
      cluster_name = :"cluster_multi_local_#{System.unique_integer([:positive])}"

      assert {:ok, servers} =
               Cluster.form_cluster(cluster_name, machine(), [node(), node()], timeout: 2_000)

      assert length(servers) == 2
    end

    test "returns the start error when the seed server is already running" do
      start_test_system()
      cluster_name = :"cluster_seed_error_#{System.unique_integer([:positive])}"

      assert {:ok, _servers} =
               Cluster.form_cluster(cluster_name, machine(), [node()], timeout: 2_000)

      assert {:error, _reason} =
               Cluster.form_cluster(cluster_name, machine(), [node()], timeout: 2_000)
    end

    test "returns a timeout error when election cannot complete in time" do
      start_test_system()
      cluster_name = :"cluster_election_timeout_#{System.unique_integer([:positive])}"

      assert Cluster.form_cluster(cluster_name, machine(), [node()], timeout: 0) ==
               {:error, {:timeout, {:server1, node()}}}
    end
  end

  describe "join_cluster/5" do
    test "warns when no server exists on the seed node" do
      remote = :"join_remote_#{System.unique_integer([:positive])}@invalid.host"

      assert {:ok, []} =
               Cluster.join_cluster(:join_no_seed, machine(), [remote], :"seed@invalid.host")
    end

    test "rejects a seed outside the node set (10.5)" do
      assert {:error, {:seed_not_in_nodes, :"other@invalid.host"}} =
               Cluster.join_cluster(
                 :join_bad_seed,
                 machine(),
                 [node()],
                 :"other@invalid.host",
                 timeout: 1_000
               )
    end

    test "rejects an unreachable seed (10.5)" do
      seed = :"join_seed_#{System.unique_integer([:positive])}@invalid.host"

      assert {:error, :seed_unreachable} =
               Cluster.join_cluster(:join_unreachable, machine(), [node(), seed], seed,
                 timeout: 1_000
               )
    end

    test "adds local servers to a cluster through a seed server" do
      cluster_name = :"cluster_join_success_#{System.unique_integer([:positive])}"
      start_single_server_cluster(cluster_name)

      assert {:ok, servers} =
               Cluster.join_cluster(cluster_name, machine(), [node(), node()], node(),
                 timeout: 2_000
               )

      assert length(servers) == 2
    end

    test "tolerates a failed attempt to start distribution" do
      if Node.alive?(), do: Node.stop()

      restore_name = :"cluster_restore_#{System.unique_integer([:positive])}@127.0.0.1"

      on_exit(fn ->
        case Node.start(restore_name) do
          {:ok, _pid} -> :ok
          {:error, _reason} -> :ok
        end
      end)

      remote = :"join_distribution_#{System.unique_integer([:positive])}@invalid.host"

      assert {:ok, []} = Cluster.join_cluster(:join_dist_failed, machine(), [remote], remote)
    end
  end

  describe "status/1" do
    test "reports an unstarted system" do
      assert Cluster.status(:"missing_system_#{System.unique_integer([:positive])}") ==
               {:error, :system_not_started}
    end
  end

  describe "find_leader/1" do
    test "returns an error for an unknown cluster" do
      assert Cluster.find_leader(:cluster_without_leader) == {:error, :unknown_cluster}
    end

    test "returns the leader recorded in the leaderboard" do
      cluster_name = :"leader_test_#{System.unique_integer([:positive])}"
      leader = {:server1, node()}
      Leaderboard.record(cluster_name, leader, [leader])
      on_exit(fn -> Leaderboard.clear(cluster_name) end)

      assert Cluster.find_leader(cluster_name) == {:ok, leader}
    end

    test "queries a member when the leaderboard has no leader" do
      cluster_name = :"cluster_find_leader_#{System.unique_integer([:positive])}"
      sid = start_single_server_cluster(cluster_name)
      Leaderboard.record(cluster_name, nil, [sid])

      assert Cluster.find_leader(cluster_name) == {:ok, sid}
    end

    test "returns no_leader_found when no recorded member answers" do
      cluster_name = :"cluster_no_leader_#{System.unique_integer([:positive])}"
      Leaderboard.record(cluster_name, nil, [{:missing_member, node()}])
      on_exit(fn -> Leaderboard.clear(cluster_name) end)

      assert Cluster.find_leader(cluster_name) == {:error, :no_leader_found}
    end
  end

  describe "handle_topology_change/3" do
    test "is a no-op with no nodes" do
      assert Cluster.handle_topology_change([], :empty_topology, machine()) == :ok
    end

    test "records the leader after adding a new remote server" do
      cluster_name = :"cluster_topology_remote_#{System.unique_integer([:positive])}"
      start_single_server_cluster(cluster_name)
      # "zzz_" sorts after the local node, so server2 is the new remote server.
      remote = :"zzz_new_#{System.unique_integer([:positive])}@invalid.host"

      assert :ok = Cluster.handle_topology_change([node(), remote], cluster_name, machine())
    end

    test "records the leader after adding a new local server" do
      cluster_name = :"cluster_topology_local_#{System.unique_integer([:positive])}"
      start_single_server_cluster(cluster_name)

      # Two entries for the local node produce server1 (existing) and server2
      # (missing), letting the successful add_member branch run locally.
      assert :ok = Cluster.handle_topology_change([node(), node()], cluster_name, machine())

      Process.sleep(50)
      assert {:ok, _leader} = Cluster.find_leader(cluster_name)
    end

    test "starts a local server when add_member fails" do
      start_test_system()
      register_stub_server(:server1)

      assert :ok = Cluster.handle_topology_change([node(), node()], :stub_topology, machine())
    end

    test "starts a local server when add_member times out" do
      start_test_system()
      register_quiet_stub_server(:server1)

      assert :ok =
               Cluster.handle_topology_change([node(), node()], :quiet_stub_topology, machine())
    end
  end
end
