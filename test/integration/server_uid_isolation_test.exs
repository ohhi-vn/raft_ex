defmodule RaftEx.ServerUidIsolationTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  setup do
    data_dir = Path.join(System.tmp_dir!(), "raft_uid_#{System.unique_integer([:positive])}")
    File.mkdir_p!(data_dir)

    RaftEx.App.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    :ok = RaftEx.start_in(data_dir)

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

    %{data_dir: data_dir}
  end

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn cmd, state -> {state, cmd} end, initial_state: 0}}
  end

  test "three single-node servers own three distinct WAL files", %{data_dir: data_dir} do
    name = :"uid_iso_#{System.unique_integer([:positive])}"
    ids = [{:u1, node()}, {:u2, node()}, {:u3, node()}]

    assert {:ok, _servers, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)

    node_dir = Path.join(data_dir, Atom.to_string(node()))

    wal_files =
      for id <- ids do
        uid = RaftEx.server_uid(name, id)
        path = Path.join([node_dir, uid, "wal.dat"])
        assert File.exists?(path), "expected WAL at #{path}"
        path
      end

    assert length(Enum.uniq(wal_files)) == 3
  end
end
