defmodule RaftEx.Infra.Persistence.SupervisorsTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.System, as: SystemConfig
  alias RaftEx.Infra.Persistence.{LogSupervisor, LogWalSupervisor}

  defp config do
    suffix = System.unique_integer([:positive])
    name = :"sup_test_#{suffix}"
    data_dir = RaftEx.TestSupport.tmp_dir("persistence_sup_test")

    %{
      name: name,
      data_dir: data_dir,
      wal_data_dir: data_dir,
      names: %{
        log_sup: :"#{name}_log_sup",
        log_meta: :"#{name}_log_meta",
        segment_writer: :"#{name}_segment_writer",
        wal: :"#{name}_wal",
        wal_sup: :"#{name}_wal_sup"
      }
    }
  end

  test "LogSupervisor starts and stops its children" do
    config = config()
    SystemConfig.store(config)
    on_exit(fn -> :persistent_term.erase({:"$ra_system", config.name}) end)

    assert {:ok, pid} = LogSupervisor.start_link(config)
    assert Process.alive?(pid)
    assert Supervisor.count_children(pid).specs == 4

    assert :ok = Supervisor.stop(pid)
  end

  test "LogWalSupervisor starts its WAL child" do
    config = config()
    wal_config = %{names: config.names, system: config.name, data_dir: config.data_dir}
    SystemConfig.store(config)
    on_exit(fn -> :persistent_term.erase({:"$ra_system", config.name}) end)

    assert {:ok, pid} = LogWalSupervisor.start_link(wal_config)
    assert Supervisor.count_children(pid).specs == 1

    assert :ok = Supervisor.stop(pid)
  end
end
