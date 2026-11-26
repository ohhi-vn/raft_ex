defmodule RaftEx.Infra.Persistence.SharedWalTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.SharedWal

  test "append/read/recover round trip" do
    dir = RaftEx.TestSupport.tmp_dir("shared_wal")
    name = :"shared_wal_#{System.unique_integer([:positive])}"

    {:ok, pid} = SharedWal.start_link(%{name: name, data_dir: dir})

    assert {:ok, 1, 2} = SharedWal.append(name, :u1, [{1, 1, :a}, {2, 1, :b}])
    assert [{1, 1, :a}, {2, 1, :b}] = SharedWal.read(name, :u1, 1, 2)

    GenServer.stop(pid)

    assert {1, 2, [{1, 1, :a}, {2, 1, :b}]} =
             SharedWal.recover(%{data_dir: dir, uid: :u1})
  end

  test "migrate from per-server files" do
    dir = RaftEx.TestSupport.tmp_dir("shared_wal_migrate")

    # Create a per-server WAL file.
    uid = :mig_uid
    uid_dir = Path.join(dir, Atom.to_string(uid))
    File.mkdir_p!(uid_dir)

    # Write a minimal per-server WAL using LogWal.
    alias RaftEx.Infra.Persistence.LogWal
    wal_name = :"mig_wal_#{System.unique_integer([:positive])}"

    {:ok, _pid} =
      LogWal.start_link(%{names: %{wal: wal_name}, data_dir: dir, wal_compute_checksums: true})

    LogWal.append(wal_name, uid, [{1, 1, :x}])
    GenServer.stop(Process.whereis(wal_name))

    assert :ok = SharedWal.migrate(dir)
    assert File.exists?(Path.join(dir, "shared.wal"))
  end
end
