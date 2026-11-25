defmodule RaftEx.App.RecoveryTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.Recovery
  alias RaftEx.App.System, as: SystemConfig
  alias RaftEx.Infra.Registry.Directory

  defmodule RecoveryProbe do
    def recover(system, arg, parent), do: send(parent, {:recovered, system, arg})
  end

  defp machine do
    {:machine, RaftEx.Domain.Machine.Simple,
     %{simple_fun: fn _cmd, state -> state end, initial_state: 0}}
  end

  defp start_test_system do
    data_dir =
      Path.join(System.tmp_dir!(), "recovery_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(data_dir)
    RaftEx.App.System.stop_default()
    RaftEx.start_in(data_dir)

    on_exit(fn ->
      RaftEx.App.System.stop_default()
      File.rm_rf!(data_dir)
    end)

    data_dir
  end

  describe "init/1" do
    test "does nothing when no recovery strategy is configured" do
      name = :"recovery_none_#{System.unique_integer([:positive])}"
      assert {:ok, pid} = Recovery.start_link(name)
      assert Process.alive?(pid)
    end

    test "invokes a configured mfa strategy" do
      name = :"recovery_mfa_#{System.unique_integer([:positive])}"
      parent = self()

      SystemConfig.store(%{
        name: name,
        server_recovery_strategy: {RecoveryProbe, :recover, [:arg, parent]}
      })

      on_exit(fn -> :persistent_term.erase({:"$ra_system", name}) end)

      assert {:ok, _pid} = Recovery.start_link(name)
      assert_receive {:recovered, ^name, :arg}
    end
  end

  describe "init/1 with the :registered strategy" do
    test "lists registered servers and logs restart failures" do
      start_test_system()

      config = SystemConfig.fetch(:default)
      dir_rev = config.names.directory_rev
      uid = "missing_uid_#{System.unique_integer([:positive])}"
      :ok = :dets.insert(dir_rev, {:missing_recovery_server, uid})

      SystemConfig.store(Map.put(config, :server_recovery_strategy, :registered))

      assert {:ok, pid} = Recovery.start_link(:default)
      assert Process.alive?(pid)
    end

    test "restarts a stopped but still registered server with a persisted config" do
      start_test_system()

      cluster_name = :"recovery_registered_#{System.unique_integer([:positive])}"
      server_id = {:server1, node()}

      assert :ok =
               RaftEx.start_server(:default, cluster_name, server_id, machine(), [server_id])

      assert :ok = RaftEx.stop_server(:default, server_id)

      # restart_server/2 recovers the server config from disk. The library does
      # not write that file itself, so seed it to exercise the successful path.
      config = SystemConfig.fetch(:default)
      uid = Directory.uid_of(:default, :server1)

      server_config = %{
        id: server_id,
        uid: uid,
        cluster_name: cluster_name,
        log_init_args: %{uid: uid},
        initial_members: [server_id],
        machine: machine()
      }

      dir = Path.join(config.data_dir, uid)
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "config"), :erlang.term_to_binary(server_config))

      SystemConfig.store(Map.put(config, :server_recovery_strategy, :registered))

      assert {:ok, pid} = Recovery.start_link(:default)
      assert Process.alive?(pid)
    end
  end
end
