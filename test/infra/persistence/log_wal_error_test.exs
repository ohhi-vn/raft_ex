defmodule RaftEx.Infra.Persistence.LogWalErrorTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogWal

  defp start_wal(base, extra \\ %{}) do
    name = :"err_wal_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      LogWal.start_link(Map.merge(%{names: %{wal: name}, data_dir: base}, extra))

    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {name, pid}
  end

  defp server_dir(base, uid), do: Path.join(base, to_string(uid))

  defmodule FailingWal do
    @moduledoc false
    use GenServer

    def start_link(reason), do: GenServer.start_link(__MODULE__, reason, [])
    def init(reason), do: {:ok, reason}

    def handle_call({:replace_from, _uid, _idx, _entries}, _from, reason),
      do: {:reply, {:error, reason}, reason}

    def handle_call(:config_probe, _from, reason), do: {:reply, :ok, reason}
  end

  test "a write failure returns an error, leaves state unchanged, and later writes succeed" do
    alias RaftEx.Infra.Persistence.Log

    {:ok, failing} = FailingWal.start_link(:wal_down)

    log = %Log{
      entries: %{1 => {1, 1, :a}, 2 => {1, 1, :b}, 3 => {1, 1, :c}},
      range: {1, 3},
      last_term: 1,
      next_index: 4,
      last_written_index_term: {3, 1},
      wal_name: failing,
      cfg: %{uid: "err-uid"},
      mem_table: nil
    }

    # The injected failure is reported and in-memory state is untouched.
    assert {:error, :wal_down} = Log.replace_from(2, [{2, 2, :new}], log)
    assert log.entries[2] == {1, 1, :b}

    # After the fault clears (real WAL), the same operation succeeds and the
    # WAL remains recoverable.
    base = RaftEx.TestSupport.tmp_dir("wal_error")
    uid = "err-uid"
    {name, _pid} = start_wal(base)
    assert {:ok, 1, 3} = LogWal.append(name, uid, [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}])

    real_log = %{log | wal_name: name}
    assert {:ok, new_log} = Log.replace_from(2, [{2, 2, :new}], real_log)
    assert new_log.entries[2] == {2, 2, :new}

    {_first, last, recovered} = LogWal.recover(%{data_dir: server_dir(base, uid)})
    assert last == 2
    assert Enum.map(recovered, &elem(&1, 0)) == [1, 2]
    assert Enum.map(recovered, &elem(&1, 1)) == [1, 2]
  end

  test "a rewrite blocked by an unreadable temp path reports an error and keeps the old file" do
    base = RaftEx.TestSupport.tmp_dir("wal_rewrite_error")
    uid = "rewrite-err-uid"
    {name, _pid} = start_wal(base)

    assert {:ok, 1, 2} = LogWal.append(name, uid, [{1, 1, :a}, {2, 1, :b}])

    # Block temp-file creation by squatting the temp path with a directory.
    server = server_dir(base, uid)
    File.mkdir_p!(server <> ".tmp")
    on_exit(fn -> File.rmdir(server <> ".tmp") end)

    # The temp path for the server file, not the server dir itself.
    tmp_dir = Path.join(server, "wal.dat.tmp")
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rmdir(tmp_dir) end)

    assert {:error, _} = LogWal.truncate(name, uid, 1)

    File.rmdir(tmp_dir)

    # The old file is untouched and the server still works.
    {_first, last, recovered} = LogWal.recover(%{data_dir: server})
    assert last == 2
    assert length(recovered) == 2

    assert :ok = LogWal.truncate(name, uid, 1)

    {_first, last2, recovered2} = LogWal.recover(%{data_dir: server})
    assert last2 == 2
    assert Enum.map(recovered2, &elem(&1, 0)) == [2]
  end
end
