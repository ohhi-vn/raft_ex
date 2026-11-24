defmodule RaftEx.App.ClusterSetupTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.ClusterSetup

  describe "start_distribution/1" do
    test "handles both distributed and non-distributed nodes" do
      if Node.alive?() do
        result = ClusterSetup.start_distribution(:raft_ex)
        assert result == :ok or match?({:error, {:already_started, _}}, result)
      else
        assert_raise ArgumentError, fn -> ClusterSetup.start_distribution(:raft_ex) end
      end
    end
  end

  describe "verify_nodes/2" do
    test "an empty node list is ready" do
      assert ClusterSetup.verify_nodes([]) == :ok
    end
  end
end
