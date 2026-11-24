defmodule RaftEx.Infra.Network.TransportTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.Transport

  test "start_monitoring without seeds" do
    if Node.alive?() do
      assert :ok = Transport.start_monitoring([])
    else
      assert_raise ErlangError, fn -> Transport.start_monitoring([]) end
    end
  end

  test "send_rpc to a registered local process" do
    name = :"transport_test_#{System.unique_integer([:positive])}"
    {:ok, _pid} = Agent.start_link(fn -> %{} end, name: name)

    assert :ok = Transport.send_rpc({name, node()}, :rpc_message)
  end

  test "local operations on a missing process" do
    missing = :"missing_#{System.unique_integer([:positive])}"

    assert {:error, :noproc} = Transport.send_rpc({missing, node()}, :rpc)
    assert {:error, :noproc} = Transport.call({missing, node()}, :call)
    assert {:error, :noproc} = Transport.cast({missing, node()}, :cast)
    assert {:error, :noproc} = Transport.monitor({missing, node()})
  end

  test "monitor on a local process" do
    name = :"transport_monitor_#{System.unique_integer([:positive])}"
    {:ok, pid} = Agent.start_link(fn -> %{} end, name: name)

    assert is_reference(Transport.monitor({name, node()}))
    assert is_pid(pid)
  end

  test "remote operations on an unreachable node" do
    remote = {:server, :"unreachable@127.0.0.1"}

    for result <- [
          Transport.send_rpc(remote, :rpc),
          Transport.call(remote, :call),
          Transport.cast(remote, :cast)
        ] do
      assert result == :ok or is_tuple(result)
    end
  end

  test "node up and down notifications" do
    assert :ok = Transport.handle_node_up(:some@node)
    assert :ok = Transport.handle_node_down(:some@node)
  end
end
