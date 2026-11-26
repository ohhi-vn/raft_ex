defmodule RaftEx.Infra.Network.NodeDiscoveryTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.NodeDiscovery
  alias RaftEx.TestSupport.DistributionHarness, as: Harness

  defmodule CallbackProbe do
    def handle_topology_change(nodes, cluster_name, parent) do
      send(parent, {:topology, nodes, cluster_name})
    end
  end

  defp ensure_distribution, do: Harness.ensure_distribution()

  # This peer is *visible* so that Distribution.cluster_nodes/0 sees it once
  # connected, which is what drives the topology-change branches.
  defp start_peer, do: Harness.start_peer("raft_ex_npeer")
  defp stop_peer(peer), do: Harness.stop_peer(peer)

  defp state do
    %NodeDiscovery{
      seeds: [],
      cookie: Harness.cookie(),
      discovery_interval: 60_000,
      cluster_name: :test_cluster,
      machine: self(),
      callback_module: CallbackProbe,
      timer_ref: nil
    }
  end

  describe "init/1" do
    test "notifies the callback when seeds connect during startup" do
      ensure_distribution()
      peer = start_peer()
      on_exit(fn -> stop_peer(peer) end)

      name = :"node_discovery_init_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NodeDiscovery.start_link(
          name: name,
          seeds: [peer.name],
          cookie: Harness.cookie(),
          discovery_interval: 60_000,
          cluster_name: :test_cluster,
          machine: self(),
          callback_module: CallbackProbe
        )

      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
      end)

      assert_receive {:topology, nodes, :test_cluster}
      assert peer.name in nodes
    end
  end

  describe "handle_info/2" do
    test ":discover with no topology change" do
      assert {:noreply, state} = NodeDiscovery.handle_info(:discover, state())
      assert is_reference(state.timer_ref)
      refute_received {:topology, _, _}
    end

    test ":discover notifies the callback when a seed joins the cluster" do
      ensure_distribution()
      peer = start_peer()
      on_exit(fn -> stop_peer(peer) end)
      Node.disconnect(peer.name)

      s = %{state() | seeds: [peer.name]}
      assert {:noreply, _state} = NodeDiscovery.handle_info(:discover, s)

      assert_receive {:topology, nodes, :test_cluster}
      assert peer.name in nodes
    end

    test "{:nodeup, ...} notifies the callback module" do
      assert {:noreply, _state} = NodeDiscovery.handle_info({:nodeup, :some@node, %{}}, state())
      assert_received {:topology, _, :test_cluster}
    end

    test "{:nodedown, ...} notifies the callback module" do
      assert {:noreply, _state} = NodeDiscovery.handle_info({:nodedown, :some@node, %{}}, state())
      assert_received {:topology, _, :test_cluster}
    end

    test "unknown messages are ignored" do
      s = state()
      assert {:noreply, ^s} = NodeDiscovery.handle_info(:unknown, s)
    end

    test "node events are ignored without a cluster and machine" do
      s = %{state() | cluster_name: nil, machine: nil}
      assert {:noreply, ^s} = NodeDiscovery.handle_info({:nodeup, :some@node, %{}}, s)
      assert {:noreply, ^s} = NodeDiscovery.handle_info({:nodedown, :some@node, %{}}, s)
    end

    test ":discover with no cluster/machine does not notify" do
      s = %{state() | seeds: [:"unknown_seed@127.0.0.1"], cluster_name: nil, machine: nil}
      assert {:noreply, _state} = NodeDiscovery.handle_info(:discover, s)
      refute_received {:topology, _, _}
    end
  end
end
