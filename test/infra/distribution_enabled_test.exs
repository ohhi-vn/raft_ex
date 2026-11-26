defmodule RaftEx.Infra.DistributionEnabledTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.ClusterSetup
  alias RaftEx.Infra.Network.{Distribution, NodeDiscovery, Transport}

  setup_all do
    case Node.start(:"raft_cov@127.0.0.1") do
      {:ok, _pid} -> :ok
      {:error, _} -> :ok
    end

    :ok
  end

  describe "Distribution" do
    test "start is idempotent when distribution is already running" do
      assert Distribution.start(cookie: :raft_ex) in [:ok, {:error, {:already_started, self()}}] or
               match?({:error, {:already_started, _}}, Distribution.start(cookie: :raft_ex))
    end

    test "connect_to_seeds connects to self and skips unreachable seeds" do
      assert Distribution.connect_to_seeds([node(), :"unknown@127.0.0.1"]) == []
      assert Distribution.connect_to_seeds([]) == []
    end
  end

  describe "ClusterSetup" do
    test "start_distribution and connect_node" do
      assert ClusterSetup.start_distribution() == :ok or
               match?({:error, {:already_started, _}}, ClusterSetup.start_distribution())

      assert ClusterSetup.connect_node(node()) == :ok
      assert ClusterSetup.connect_node(:"unknown@127.0.0.1") == {:error, :connection_failed}
    end

    test "verify_nodes reports readiness" do
      assert ClusterSetup.verify_nodes([]) == :ok

      this_node = node()

      assert {:error, {:nodes_not_ready, results}} =
               ClusterSetup.verify_nodes([this_node, :"unknown@127.0.0.1"])

      by_node = Map.new(results, &{&1.node, &1})
      refute by_node[:"unknown@127.0.0.1"].reachable

      # A reachable node that already runs RaftEx passes verification and is
      # filtered out of the failure list, so it may be absent here.
      case by_node[this_node] do
        nil -> :ok
        result -> assert result.reachable
      end
    end
  end

  describe "Transport and NodeDiscovery" do
    test "start_monitoring succeeds with distribution" do
      assert :ok = Transport.start_monitoring([])
    end

    test "NodeDiscovery starts with seeds" do
      name = :"node_discovery_cov_#{System.unique_integer([:positive])}"

      assert {:ok, pid} =
               NodeDiscovery.start_link(
                 name: name,
                 seeds: [node()],
                 discovery_interval: 60_000,
                 cluster_name: nil,
                 machine: nil
               )

      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end
end
