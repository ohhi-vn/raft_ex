defmodule RaftEx.App.ClusterSetupTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.ClusterSetup

  setup_all do
    name = :"cluster_setup_#{System.unique_integer([:positive])}@127.0.0.1"

    case Node.start(name) do
      {:ok, _pid} -> :ok
      {:error, _reason} -> :ok
    end

    :ok
  end

  defp machine do
    {:machine, RaftEx.Domain.Machine.Simple,
     %{simple_fun: fn _cmd, state -> state end, initial_state: 0}}
  end

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

  describe "setup/4" do
    test "starts the system and forms a cluster with the local node config" do
      data_dir =
        Path.join(System.tmp_dir!(), "cluster_setup_#{System.unique_integer([:positive])}")

      File.mkdir_p!(data_dir)

      on_exit(fn ->
        RaftEx.App.System.stop_default()
        File.rm_rf!(data_dir)
      end)

      cluster_name = :"cluster_setup_local_#{System.unique_integer([:positive])}"
      node_configs = [%{node: node(), data_dir: data_dir}]

      assert {:ok, [sid], all_ids} =
               ClusterSetup.setup(cluster_name, machine(), node_configs, timeout: 2_000)

      assert sid == {:server1, node()}
      assert all_ids == [{:server1, node()}]
    end

    test "uses a fallback data dir and propagates a form_cluster error" do
      remote = :"cluster_setup_remote_#{System.unique_integer([:positive])}@invalid.host"
      cluster_name = :"cluster_setup_remote_#{System.unique_integer([:positive])}"

      on_exit(fn -> RaftEx.App.System.stop_default() end)

      assert {:error, :no_local_servers} =
               ClusterSetup.setup(cluster_name, machine(), [
                 %{node: remote, data_dir: "/tmp/raft_ex_unused"}
               ])
    end
  end
end
