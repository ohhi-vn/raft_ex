defmodule RaftEx.Infra.SupervisorTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Supervisor.{ServerSupSupervisor, SystemsSupervisor, SystemSupervisor}

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
               ServerSupSupervisor.stop_server_rpc(system, :server)

      assert {:error, :system_not_started} =
               ServerSupSupervisor.delete_server_rpc(system, :server)
    end

    test "stop_server propagates RPC errors" do
      name = :"stop_error_server_#{System.unique_integer([:positive])}"

      assert {:error, :system_not_started} =
               ServerSupSupervisor.stop_server(:system_that_is_not_started, {name, node()})

      assert {:error, {:badrpc, :nodedown}} =
               ServerSupSupervisor.stop_server(:default, {name, :"no_such_node@127.0.0.1"})
    end

    test "find_child ignores children that are not supervisors" do
      system = :"fake_system_#{System.unique_integer([:positive])}"
      srv_sup = :"fake_server_sup_#{System.unique_integer([:positive])}"

      :persistent_term.put({:"$ra_system", system}, %{name: system, names: %{server_sup: srv_sup}})

      on_exit(fn -> :persistent_term.erase({:"$ra_system", system}) end)

      {:ok, _sup_pid} = DynamicSupervisor.start_link(name: srv_sup)

      {:ok, _task} =
        DynamicSupervisor.start_child(srv_sup, {Task, fn -> Process.sleep(:infinity) end})

      assert :ok = ServerSupSupervisor.stop_server_rpc(system, :a_worker)
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

    test "starting a system twice returns an error" do
      {name, config} = system_config()

      {:ok, pid} = SystemsSupervisor.start_system(config)

      on_exit(fn ->
        SystemsSupervisor.stop_system(name)
        :persistent_term.erase({:"$ra_system", name})
      end)

      assert {:error, _reason} = SystemsSupervisor.start_system(config)
      assert is_pid(pid)
    end

    test "stop_system accepts a config map and finds a child without a stored pid" do
      {name, config} = system_config()

      {:ok, _pid} = SystemsSupervisor.start_system(config)
      :persistent_term.erase({:"$ra_system_pid", name})

      assert :ok = SystemsSupervisor.stop_system(%{name: name})
      assert :persistent_term.get({:"$ra_system_pid", name}, nil) == nil
    end

    defp system_config do
      name = :"sys_sup_test_#{System.unique_integer([:positive])}"
      data_dir = RaftEx.TestSupport.tmp_dir("systems_supervisor_test")
      base = RaftEx.App.System.default_config()

      config = %{
        base
        | name: name,
          data_dir: data_dir,
          wal_data_dir: data_dir,
          names: RaftEx.App.System.derive_names(name)
      }

      {name, config}
    end
  end
end
