defmodule RaftEx.Infra.Network.NodeMonitorTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.NodeMonitor

  defp ensure_distribution do
    if Node.alive?() do
      Node.set_cookie(:raft_ex)
    else
      unique = System.unique_integer([:positive])

      case Node.start(:"raft_ex_monitor_#{unique}@127.0.0.1") do
        {:ok, _pid} -> Node.set_cookie(:raft_ex)
        {:error, _reason} -> :ok
      end
    end
  end

  test "starts and ignores unknown messages" do
    name = :"node_monitor_test_#{System.unique_integer([:positive])}"
    assert {:ok, pid} = NodeMonitor.start_link(name: name)

    send(pid, :unknown)
    send(pid, {:unknown, :payload})

    assert Process.alive?(pid)
  end

  test "init monitors nodes when distribution is alive" do
    ensure_distribution()

    assert {:ok, %{alive: true}} = NodeMonitor.init([])
  end

  test "forwards node events to the transport" do
    name = :"node_monitor_events_#{System.unique_integer([:positive])}"
    {:ok, pid} = NodeMonitor.start_link(name: name)

    send(pid, {:nodeup, :some@node, %{}})
    send(pid, {:nodedown, :some@node, %{}})

    ref = Process.monitor(pid)
    GenServer.stop(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
  end
end
