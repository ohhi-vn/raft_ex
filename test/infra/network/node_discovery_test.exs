defmodule RaftEx.Infra.Network.NodeDiscoveryTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.NodeDiscovery

  defmodule CallbackProbe do
    def handle_topology_change(nodes, cluster_name, parent) do
      send(parent, {:topology, nodes, cluster_name})
    end
  end

  defp state do
    %NodeDiscovery{
      seeds: [],
      cookie: :raft_ex,
      discovery_interval: 60_000,
      cluster_name: :test_cluster,
      machine: self(),
      callback_module: CallbackProbe,
      timer_ref: nil
    }
  end

  describe "handle_info/2" do
    test ":discover with no topology change" do
      assert {:noreply, state} = NodeDiscovery.handle_info(:discover, state())
      assert is_reference(state.timer_ref)
      refute_received {:topology, _, _}
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
    end
  end
end
