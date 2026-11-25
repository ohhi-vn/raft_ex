defmodule RaftEx.Infra.Network.NodeDiscoveryTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.NodeDiscovery

  defmodule CallbackProbe do
    def handle_topology_change(nodes, cluster_name, parent) do
      send(parent, {:topology, nodes, cluster_name})
    end
  end

  defp ensure_distribution do
    if Node.alive?() do
      Node.set_cookie(:raft_ex)
    else
      unique = System.unique_integer([:positive])

      case Node.start(:"raft_ex_discovery_#{unique}@127.0.0.1") do
        {:ok, _pid} -> Node.set_cookie(:raft_ex)
        {:error, _reason} -> :ok
      end
    end
  end

  # This peer is *visible* so that Distribution.cluster_nodes/0 sees it once
  # connected, which is what drives the topology-change branches.
  defp start_peer do
    unique = System.unique_integer([:positive])
    name = :"raft_ex_npeer_#{unique}@127.0.0.1"
    ctl = :"raft_ex_npeer_ctl_#{unique}"

    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        :hide,
        args: [
          "--name",
          Atom.to_string(name),
          "--cookie",
          "raft_ex",
          "-e",
          "Process.register(self(), #{inspect(ctl)}); IO.puts(\"READY\"); " <>
            "receive do :stop -> :ok after 30_000 -> :ok end"
        ]
      ])

    await_ready(port)
    %{name: name, ctl: ctl, port: port}
  end

  defp await_ready(port, acc \\ "") do
    receive do
      {^port, {:data, data}} ->
        acc = acc <> data

        if String.contains?(acc, "READY") do
          :ok
        else
          await_ready(port, acc)
        end

      {^port, {:exit_status, status}} ->
        raise "discovery peer exited before ready (status #{status}): #{acc}"
    after
      15_000 -> raise "discovery peer did not become ready: #{acc}"
    end
  end

  defp stop_peer(%{name: name, ctl: ctl, port: port}) do
    if Node.alive?() do
      send({ctl, name}, :stop)
      Node.disconnect(name)
    end

    try do
      Port.close(port)
    rescue
      _ -> :ok
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
          cookie: :raft_ex,
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
