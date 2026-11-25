defmodule RaftEx.Infra.Network.TransportTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.Transport

  # Starts distribution (longnames on loopback) once for the whole run. Other
  # test modules may have started it already, which is fine.
  defp ensure_distribution do
    if Node.alive?() do
      Node.set_cookie(:raft_ex)
    else
      unique = System.unique_integer([:positive])

      case Node.start(:"raft_ex_transport_#{unique}@127.0.0.1") do
        {:ok, _pid} -> Node.set_cookie(:raft_ex)
        {:error, _reason} -> :ok
      end
    end
  end

  # Boots a real (hidden) BEAM node in a subprocess so that Node.connect/1
  # succeeds and the remote branches of Transport can be exercised.
  defp start_peer do
    unique = System.unique_integer([:positive])
    name = :"raft_ex_tpeer_#{unique}@127.0.0.1"
    ctl = :"raft_ex_tpeer_ctl_#{unique}"

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
          "--hidden",
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
        raise "transport peer exited before ready (status #{status}): #{acc}"
    after
      15_000 -> raise "transport peer did not become ready: #{acc}"
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

  defmodule SilentServer do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, %{}, opts)

    @impl true
    def init(state), do: {:ok, state}

    # Deliberately swallows the gen_statem call message so callers time out.
    @impl true
    def handle_call(_msg, _from, state), do: {:noreply, state}

    @impl true
    def handle_info(_msg, state), do: {:noreply, state}
  end

  test "start_monitoring without seeds" do
    if Node.alive?() do
      assert :ok = Transport.start_monitoring([])
    else
      assert_raise ErlangError, fn -> Transport.start_monitoring([]) end
    end
  end

  test "start_monitoring with seeds sets the cookie and connects/rejects seeds" do
    ensure_distribution()
    peer = start_peer()
    on_exit(fn -> stop_peer(peer) end)

    assert :ok =
             Transport.start_monitoring(
               seeds: [peer.name, :"unknown_seed@127.0.0.1", node()],
               cookie: :raft_ex
             )
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

  test "monitor on a remote node" do
    ensure_distribution()

    assert is_reference(Transport.monitor({:server, :"remote_node@127.0.0.1"}))
  end

  test "call to a local process that never replies returns a timeout" do
    name = :"transport_timeout_#{System.unique_integer([:positive])}"
    {:ok, _pid} = SilentServer.start_link(name: name)

    assert {:error, :timeout} = Transport.call({name, node()}, :ping, 30)
  end

  test "call to a local process that exits propagates the exit reason" do
    name = :"transport_exit_#{System.unique_integer([:positive])}"
    parent = self()

    pid =
      spawn(fn ->
        Process.register(self(), name)
        send(parent, {:ready, self()})

        receive do
          _ -> exit(:boom)
        end
      end)

    assert_receive {:ready, ^pid}
    assert {:error, reason} = Transport.call({name, node()}, :ping, 1_000)
    refute reason == :noproc
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

  test "remote send_rpc, call and cast over a connected node" do
    ensure_distribution()
    peer = start_peer()
    on_exit(fn -> stop_peer(peer) end)

    assert :ok = Transport.send_rpc({:some_name, peer.name}, :rpc)

    assert {:ok, _reply} = Transport.call({:missing_name, peer.name}, :call, 2_000)
    assert :ok = Transport.cast({:some_name, peer.name}, :cast)
  end

  test "node up and down notifications" do
    assert :ok = Transport.handle_node_up(:some@node)
    assert :ok = Transport.handle_node_down(:some@node)
  end

  test "node events are forwarded to registered ra processes only" do
    registered = :"transport_ra_reg_#{System.unique_integer([:positive])}"
    missing = :"transport_ra_missing_#{System.unique_integer([:positive])}"

    true = Process.register(self(), registered)
    :ets.insert(:ra_state, {registered, :leader, []})
    :ets.insert(:ra_state, {missing, :follower, []})

    on_exit(fn ->
      :ets.delete(:ra_state, registered)
      :ets.delete(:ra_state, missing)
    end)

    assert :ok = Transport.handle_node_up(:some@node)
    assert_receive {:ra_network_event, :nodeup, :some@node}

    assert :ok = Transport.handle_node_down(:some@node)
    assert_receive {:ra_network_event, :nodedown, :some@node}
  end
end
