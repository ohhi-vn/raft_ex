defmodule RaftEx.Infra.Network.DistributionTest do
  use ExUnit.Case, async: true

  alias RaftEx.Infra.Network.Distribution

  describe "connect_to_seeds/2" do
    test "skips connection when the node is not alive" do
      assert Distribution.connect_to_seeds([:seed@host]) == []
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
