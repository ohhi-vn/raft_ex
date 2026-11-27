defmodule RaftEx.Infra.Persistence.LogWalReplaceTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.{Log, LogWal}

  defp start_wal(base) do
    name = :"replace_wal_#{System.unique_integer([:positive])}"
    {:ok, pid} = LogWal.start_link(%{names: %{wal: name}, data_dir: base})
    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {name, pid}
  end

  test "replace_from atomically swaps the suffix" do
    base = RaftEx.TestSupport.tmp_dir("wal_replace")
    uid = "replace-uid"
    {name, pid} = start_wal(base)

    for i <- 1..5 do
      assert {:ok, _, _} = LogWal.append(name, uid, [{i, 1, {:cmd, i}}])
    end

    assert :ok = LogWal.replace_from(name, uid, 4, [{4, 2, :new}, {5, 2, :new}])
    GenServer.stop(pid)

    server_dir = Path.join(base, uid)
    {_first, last, recovered} = LogWal.recover(%{data_dir: server_dir})
    assert last == 5
    assert Enum.map(recovered, &elem(&1, 0)) == [1, 2, 3, 4, 5]
    assert Enum.map(recovered, &elem(&1, 1)) == [1, 1, 1, 2, 2]

    # No temp file left behind after success.
    refute File.exists?(Path.join(server_dir, "wal.dat.tmp"))
  end

  test "crash before rename keeps the old log; crash after rename keeps the new log" do
    base = RaftEx.TestSupport.tmp_dir("wal_replace_crash")
    uid = "crash-uid"
    {name, pid} = start_wal(base)

    for i <- 1..3 do
      assert {:ok, _, _} = LogWal.append(name, uid, [{i, 1, {:cmd, i}}])
    end

    GenServer.stop(pid)
    server_dir = Path.join(base, uid)
    path = Path.join(server_dir, "wal.dat")

    # Simulate a crash after the temp file was written but before rename:
    # the live file is untouched, so recovery keeps the old log.
    File.write!(path <> ".tmp", "partial-temp-bytes")
    {_first, last, recovered} = LogWal.recover(%{data_dir: server_dir})
    assert last == 3
    assert length(recovered) == 3
    File.rm(path <> ".tmp")

    # Now do a real atomic replace (crash after rename keeps the new log).
    {name2, pid2} = start_wal(base)
    assert :ok = LogWal.replace_from(name2, uid, 2, [{2, 2, :new}, {3, 2, :new}])
    GenServer.stop(pid2)

    {_first, last2, recovered2} = LogWal.recover(%{data_dir: server_dir})
    assert last2 == 3
    assert Enum.map(recovered2, &elem(&1, 1)) == [1, 2, 2]
  end

  test "Log.replace_from updates memory only when the WAL rewrite succeeds" do
    base = RaftEx.TestSupport.tmp_dir("wal_replace_log")
    uid = "log-replace-uid"
    {name, pid} = start_wal(base)

    assert {:ok, _, _} = LogWal.append(name, uid, [{1, 1, :a}, {2, 1, :b}])

    log = %Log{
      entries: %{1 => {1, 1, :a}, 2 => {1, 1, :b}},
      range: {1, 2},
      last_term: 1,
      next_index: 3,
      last_written_index_term: {2, 1},
      wal_name: name,
      cfg: %{uid: uid},
      mem_table: nil
    }

    assert {:ok, new_log} = Log.replace_from(2, [{2, 2, :new}], log)
    assert new_log.entries[2] == {2, 2, :new}
    assert new_log.entries[1] == {1, 1, :a}
    GenServer.stop(pid)

    {_first, _last, recovered} = LogWal.recover(%{data_dir: Path.join(base, uid)})
    assert Enum.map(recovered, &elem(&1, 0)) == [1, 2]
    assert Enum.map(recovered, &elem(&1, 1)) == [1, 2]
  end
end
