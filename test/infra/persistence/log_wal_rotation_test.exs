defmodule RaftEx.Infra.Persistence.LogWalRotationTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogWal

  test "rotation keeps every entry once across three files in index order" do
    base = RaftEx.TestSupport.tmp_dir("wal_rotation")
    uid = "rotate-uid"
    name = :"rotate_wal_#{System.unique_integer([:positive])}"

    # Small max size forces several rotations over 60 entries.
    {:ok, pid} =
      LogWal.start_link(%{
        names: %{wal: name},
        data_dir: base,
        wal_max_size_bytes: 2_000,
        wal_max_batch_size: 5
      })

    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    for i <- 1..60 do
      payload = :binary.copy(<<i::8>>, 32)
      assert {:ok, _, _} = LogWal.append(name, uid, [{i, 1, {:cmd, payload}}])
    end

    GenServer.stop(pid)

    server_dir = Path.join(base, to_string(uid))
    {:ok, names} = File.ls(server_dir)
    wal_files = Enum.filter(names, &(&1 == "wal.dat" or String.match?(&1, ~r/^wal\.\d+\.dat$/)))
    assert length(wal_files) >= 3, "expected at least 3 WAL files, got #{inspect(wal_files)}"

    {_first, last, recovered} = LogWal.recover(%{data_dir: server_dir})
    assert last == 60
    assert Enum.map(recovered, &elem(&1, 0)) == Enum.to_list(1..60)

    # No duplicates.
    assert length(Enum.uniq(Enum.map(recovered, &elem(&1, 0)))) == 60
  end
end
