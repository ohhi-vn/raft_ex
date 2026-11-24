defmodule RaftEx.Infra.SupervisorTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Supervisor.{ServerSupSupervisor, SystemSupervisor, SystemsSupervisor}

  describe "ServerSupSupervisor" do
    test "init returns a one_for_one dynamic supervisor spec" do
      assert {:ok, %{strategy: :one_for_one}} = ServerSupSupervisor.init(%{})
    end

    test "exposes mutable config keys" do
      assert :max_pipeline_count in ServerSupSupervisor.mutable_config_keys()
      assert :await_condition_timeout in ServerSupSupervisor.mutable_config_keys()
    end

    test "rpc helpers report an unstarted system" do
      system = :"missing_sup_system_#{System.unique_integer([:positive])}"

      assert {:error, :system_not_started} =
               ServerSupSupervisor.start_server_rpc(system, "uid", %{
                 id: {:server, node()},
                 uid: "uid"
               })

      assert {:error, :system_not_started} =
               ServerSupSupervisor.restart_server_rpc(system, {:server, node()}, %{})

      assert {:error, :system_not_started} =
               ServerSupSupervisor.prepare_server_stop_rpc(system, :server)

      assert {:error, :system_not_started} =
               ServerSupSupervisor.delete_server_rpc(system, :server)
    end
  end

  describe "SystemSupervisor" do
    test "init builds a one_for_all supervisor" do
      data_dir = RaftEx.TestSupport.tmp_dir("system_supervisor_test")

      assert {:ok, {%{strategy: :one_for_all}, children}} =
               SystemSupervisor.init(%{data_dir: data_dir, name: :test_system})

      assert length(children) == 4
    end

    test "init raises when the data directory cannot be created" do
      assert_raise RuntimeError, fn ->
        SystemSupervisor.init(%{data_dir: "/dev/null/impossible", name: :bad_system})
      end
    end
  end

  describe "SystemsSupervisor" do
    test "init returns a one_for_one dynamic supervisor" do
      assert {:ok, %{strategy: :one_for_one}} = SystemsSupervisor.init([])
    end

    test "stopping an unknown system is a no-op" do
      name = :"never_started_#{System.unique_integer([:positive])}"
      assert :ok = SystemsSupervisor.stop_system(name)
    end
  end
end
