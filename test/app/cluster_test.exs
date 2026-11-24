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

  describe "form_cluster/4" do
    test "returns an error when no nodes are local" do
      assert {:error, :no_local_servers} =
               Cluster.form_cluster(:remote_cluster, machine(), [:"remote@invalid.host"])
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
  end

  describe "handle_topology_change/3" do
    test "is a no-op with no nodes" do
      assert Cluster.handle_topology_change([], :empty_topology, machine()) == :ok
    end
  end
end
