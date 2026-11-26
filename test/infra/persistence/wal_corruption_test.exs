defmodule RaftEx.Infra.Persistence.WalCorruptionTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogWal

  @uid :wal_corruption_uid

  test "truncated/corrupt WAL tail is ignored during recovery" do
    dir = RaftEx.TestSupport.tmp_dir("wal_corrupt")
    uid = @uid

    config = %{
      names: %{wal: :"wal_corrupt_#{System.unique_integer([:positive])}"},
      data_dir: dir,
      wal_compute_checksums: true
    }

    name = config.names.wal
    {:ok, pid} = LogWal.start_link(config)

    LogWal.append(name, uid, [{1, 1, :a}, {2, 1, :b}])
    GenServer.stop(pid)

    # Append garbage bytes to simulate a corrupt/truncated tail.
    path = Path.join([dir, Atom.to_string(uid), "wal.dat"])
    File.write!(path, "GARBAGE", [:append])

    assert {1, 2, [{1, 1, :a}, {2, 1, :b}]} =
             LogWal.recover(%{data_dir: Path.join(dir, Atom.to_string(uid))})
  end
end
