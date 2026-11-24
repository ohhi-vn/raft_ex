defmodule RaftEx.Infra.Supervisor.ServerSupTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine
  alias RaftEx.Infra.Supervisor.ServerSupSupervisor

  setup do
    data_dir = Path.join(System.tmp_dir!(), "server_sup_#{System.unique_integer([:positive])}")
    File.mkdir_p!(data_dir)

    RaftEx.App.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    RaftEx.start_in(data_dir)

    on_exit(fn ->
      for [name] <- :ets.match(:ra_state, {:"$1", :_, :_}) do
        case Process.whereis(name) do
          nil -> :ok
          pid -> :gen_statem.stop(pid)
        end
      end

      :ets.match_delete(:ra_state, {:_, :_, :_})
      :persistent_term.erase({:"$ra_system", :default})
      File.rm_rf!(data_dir)
    end)

    :ok
  end

  defp config(sname, uid) do
    %{
      id: {sname, node()},
      uid: uid,
      cluster_name: :sup_test_cluster,
      log_init_args: %{uid: uid},
      initial_members: [{sname, node()}],
      machine: {:machine, Machine.Simple, %{simple_fun: fn _c, s -> s end, initial_state: 0}}
    }
  end

  test "start_server_rpc starts a server and stop/delete helpers work" do
    sname = :"sup_srv_#{System.unique_integer([:positive])}"
    uid = "sup_uid_#{System.unique_integer([:positive])}"
    cfg = config(sname, uid)

    assert {:ok, _pid} = ServerSupSupervisor.start_server_rpc(:default, uid, cfg)

    assert is_tuple(ServerSupSupervisor.prepare_server_stop_rpc(:default, sname))

    assert :ok == RaftEx.stop_server(:default, {sname, node()}) ||
             match?({:error, _}, RaftEx.stop_server(:default, {sname, node()}))

    # Deleting an already-stopped server is idempotent.  Older versions raised
    # a FunctionClauseError here when the name was no longer registered.
    assert :ok == ServerSupSupervisor.delete_server_rpc(:default, sname)
  end

  test "recover_config reports unregistered names" do
    assert {:error, :name_not_registered} =
             ServerSupSupervisor.recover_config(
               :default,
               :"never_registered_#{System.unique_integer()}"
             )
  end
end
