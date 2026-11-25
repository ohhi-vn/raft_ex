defmodule RaftEx.Infra.Network.DistributionTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.Distribution

  # Distribution is global state: start it once with a unique loopback name and
  # reuse it for every test in this module (and the rest of the suite).
  defp ensure_distribution do
    if Node.alive?() do
      Node.set_cookie(:raft_ex)
    else
      unique = System.unique_integer([:positive])

      case Node.start(:"raft_ex_dist_#{unique}@127.0.0.1") do
        {:ok, _pid} -> Node.set_cookie(:raft_ex)
        {:error, _reason} -> :ok
      end
    end
  end

  defp start_peer do
    unique = System.unique_integer([:positive])
    name = :"raft_ex_dpeer_#{unique}@127.0.0.1"
    ctl = :"raft_ex_dpeer_ctl_#{unique}"

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
        raise "distribution peer exited before ready (status #{status}): #{acc}"
    after
      15_000 -> raise "distribution peer did not become ready: #{acc}"
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

  describe "connect_to_seeds/2" do
    test "skips connection when the node is not alive" do
      assert Distribution.connect_to_seeds([:seed@host]) == []
    end

    test "tolerates a cookie that cannot be set" do
      ensure_distribution()

      assert Distribution.connect_to_seeds([], "not-an-atom-cookie") == []
    end

    test "connects to reachable seeds and skips self" do
      ensure_distribution()
      peer = start_peer()
      on_exit(fn -> stop_peer(peer) end)

      assert Distribution.connect_to_seeds([peer.name, node()]) == [peer.name]
    end

    test "skips seeds that raise while connecting" do
      ensure_distribution()

      assert Distribution.connect_to_seeds([123]) == []
    end
  end

  describe "start/1" do
    test "returns an error tuple when distribution is already running" do
      ensure_distribution()

      assert {:error, {:already_started, _pid}} = Distribution.start(cookie: :raft_ex)
    end
  end

  describe "cluster_nodes/0" do
    test "includes the local node" do
      assert node() in Distribution.cluster_nodes()
    end
  end

  describe "build_server_ids/2" do
    test "sorts nodes and numbers them deterministically" do
      assert Distribution.build_server_ids([:c@host, :a@host, :b@host]) == [
               {:server1, :a@host},
               {:server2, :b@host},
               {:server3, :c@host}
             ]
    end

    test "accepts a custom prefix" do
      assert Distribution.build_server_ids([:b@host, :a@host], :raft) == [
               {:raft1, :a@host},
               {:raft2, :b@host}
             ]
    end

    test "empty input" do
      assert Distribution.build_server_ids([]) == []
    end
  end
end
