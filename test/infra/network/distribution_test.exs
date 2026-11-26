defmodule RaftEx.Infra.Network.DistributionTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.Distribution
  alias RaftEx.TestSupport.DistributionHarness, as: Harness

  # Distribution is global state, so reuse whatever node the suite started and
  # let the harness adapt peer names to short/long name mode.
  defp ensure_distribution, do: Harness.ensure_distribution()
  defp start_peer, do: Harness.start_peer("raft_ex_dpeer", hidden: true)
  defp stop_peer(peer), do: Harness.stop_peer(peer)

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

      result = Distribution.start(cookie: :raft_ex)
      assert result == :ok or match?({:error, {:already_started, _}}, result)
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
