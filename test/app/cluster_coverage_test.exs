defmodule RaftEx.App.ClusterCoverageTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.Cluster
  alias RaftEx.Leaderboard

  defp machine do
    {:machine, RaftEx.Domain.Machine.Simple,
     %{simple_fun: fn _cmd, state -> state end, initial_state: 0}}
  end

  setup do
    Leaderboard.init()
    cluster_name = :"cluster_cov_#{System.unique_integer([:positive])}"
    on_exit(fn -> Leaderboard.clear(cluster_name) end)
    %{cluster_name: cluster_name}
  end

  defp start_test_system do
    data_dir =
      Path.join(System.tmp_dir!(), "cluster_cov_#{System.unique_integer([:positive])}")

    File.mkdir_p!(data_dir)
    RaftEx.App.System.stop_default()
    RaftEx.start_in(data_dir)

    on_exit(fn ->
      RaftEx.App.System.stop_default()
      File.rm_rf!(data_dir)
    end)

    :ok
  end

  test "join_cluster reports an unreachable seed when local servers exist", %{cluster_name: name} do
    start_test_system()
    remote = :"aaa_down_seed_#{System.unique_integer([:positive])}@invalid.host"

    assert {:error, :seed_unreachable} =
             Cluster.join_cluster(name, machine(), [remote, node()], remote, timeout: 1_000)
  end

  test "join_cluster is a no-op when the joining node hosts no servers", %{cluster_name: name} do
    remote = :"no_local_seed_#{System.unique_integer([:positive])}@invalid.host"

    assert {:ok, []} = Cluster.join_cluster(name, machine(), [remote], remote)
  end

  test "join_cluster can bootstrap a fresh single-node cluster", %{cluster_name: name} do
    start_test_system()
    this_node = node()

    assert {:ok, [{_server, ^this_node} = sid]} =
             Cluster.join_cluster(name, machine(), [this_node], this_node, timeout: 5_000)

    assert {:pong, _role} = RaftEx.ping(sid, 2_000)
  end

  test "find_leader reports an unknown cluster with no matching servers" do
    start_test_system()
    name = :"missing_cluster_#{System.unique_integer([:positive])}"

    assert {:error, :unknown_cluster} = Cluster.find_leader(name)
  end

  test "status lists servers after a cluster is formed", %{cluster_name: name} do
    start_test_system()

    assert {:ok, [sid]} = Cluster.form_cluster(name, machine(), [node()], timeout: 5_000)
    assert {:ok, %{node: local_node, servers: servers}} = Cluster.status()
    assert local_node == node()
    assert Enum.any?(servers, &(&1.server == elem(sid, 0)))
  end

  test "form_cluster rejects a cluster with no local participants" do
    remote = :"remote_only_#{System.unique_integer([:positive])}@invalid.host"

    assert {:error, :no_local_servers} = Cluster.form_cluster(:remote_only, machine(), [remote])
  end
end
