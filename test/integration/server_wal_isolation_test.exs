defmodule RaftEx.ServerWalIsolationTest do
  @moduledoc """
  Task 3.10: per-server WAL isolation across restarts.

  A three-server single-node cluster processes commands, then each server
  is stopped and restarted with its own derived uid. Every server must
  recover exactly its own log prefix (same commands, same order), the three
  WAL files must stay distinct, and the cluster must keep serving commands.
  """
  use ExUnit.Case, async: false

  defmodule AccMachine do
    @behaviour RaftEx.Domain.Machine

    def init(_conf), do: []
    def apply(_meta, cmd, state), do: {state ++ [cmd], :ok}
    def tick(state), do: state
    def version(state), do: state
    def aux(state), do: state
  end

  setup do
    data_dir = Path.join(System.tmp_dir!(), "raft_wal_iso_#{System.unique_integer([:positive])}")
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

  defp machine, do: {:machine, AccMachine, %{}}

  defp restart_server(name, id, ids) do
    assert :ok = RaftEx.stop_server(:default, id)
    Process.sleep(200)

    uid = RaftEx.server_uid(name, id)

    config = %{
      id: id,
      uid: uid,
      cluster_name: name,
      log_init_args: %{uid: uid},
      initial_members: ids,
      machine: machine()
    }

    assert :ok = RaftEx.start_server(:default, config)
  end

  defp machine_state(id) do
    {:ok, state, _} = RaftEx.local_query(id, fn s -> s end, 5_000)
    state
  end

  test "each restarted server recovers only its own entries", %{data_dir: data_dir} do
    name = :"wal_iso_#{System.unique_integer([:positive])}"
    ids = [{:w1, node()}, {:w2, node()}, {:w3, node()}]

    assert {:ok, _servers, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)

    leader =
      Enum.find_value(ids, fn id ->
        case RaftEx.process_command(id, {:probe, System.unique_integer()}, 5_000) do
          {:ok, _, found} -> found
          _ -> nil
        end
      end)

    assert leader != nil

    base = machine_state(leader)

    for i <- 1..5 do
      assert {:ok, _, _} = RaftEx.process_command(leader, {:cmd, i}, 5_000)
    end

    expected = base ++ Enum.map(1..5, &{:cmd, &1})
    for id <- ids, do: assert(machine_state(id) == expected)

    node_dir = Path.join(data_dir, Atom.to_string(node()))

    for id <- ids do
      restart_server(name, id, ids)
      Process.sleep(2_000)

      # The restarted server recovered its own log prefix: same commands.
      assert machine_state(id) == expected,
             "server #{inspect(id)} did not recover its own entries"

      # Its WAL file holds exactly this server's log.
      uid = RaftEx.server_uid(name, id)

      {_first, last, recovered} =
        RaftEx.Infra.Persistence.LogWal.recover(%{data_dir: Path.join(node_dir, uid)})

      indexes = Enum.map(recovered, &elem(&1, 0))
      assert indexes == Enum.to_list(1..last)
      # WAL holds the leader no-ops plus every applied command.
      assert last >= length(expected) + 1
    end

    # The three WAL files are distinct files.
    paths =
      for id <- ids do
        uid = RaftEx.server_uid(name, id)
        path = Path.join([node_dir, uid, "wal.dat"])
        assert File.exists?(path)
        path
      end

    assert length(Enum.uniq(paths)) == 3

    # The cluster still serves commands after all restarts.
    new_leader =
      Enum.find_value(1..30, fn _ ->
        Process.sleep(500)

        Enum.find_value(ids, fn id ->
          case RaftEx.process_command(id, {:cmd, 6}, 2_000) do
            {:ok, _, found} -> found
            _ -> nil
          end
        end)
      end)

    assert new_leader != nil
  end
end
