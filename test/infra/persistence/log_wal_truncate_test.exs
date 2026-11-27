defmodule RaftEx.Infra.Persistence.LogWalTruncateTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogWal

  defp start_wal(base) do
    name = :"truncate_wal_#{System.unique_integer([:positive])}"
    {:ok, pid} = LogWal.start_link(%{names: %{wal: name}, data_dir: base})
    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {name, pid}
  end

  test "prefix truncation removes bytes from disk and restart recovers only retained entries" do
    base = RaftEx.TestSupport.tmp_dir("wal_truncate_prefix")
    uid = "trunc-prefix-uid"
    {name, pid} = start_wal(base)

    for i <- 1..10 do
      assert {:ok, _, _} = LogWal.append(name, uid, [{i, 1, {:cmd, i}}])
    end

    path = Path.join([base, to_string(uid), "wal.dat"])
    {:ok, before_size} = File.stat(path) |> then(fn {:ok, s} -> {:ok, s.size} end)

    assert :ok = LogWal.truncate(name, uid, 5)

    {:ok, after_size} = File.stat(path) |> then(fn {:ok, s} -> {:ok, s.size} end)
    assert after_size < before_size, "truncation must remove bytes from disk"

    # Restart: stop the server and recover from disk.
    GenServer.stop(pid)
    server_dir = Path.join(base, to_string(uid))

    assert {6, 10, recovered} = LogWal.recover(%{data_dir: server_dir})
    assert Enum.map(recovered, &elem(&1, 0)) == Enum.to_list(6..10)

    # A fresh server on the same directory can keep appending after truncation.
    {name2, pid2} = start_wal(base)
    assert {:ok, 11, 11} = LogWal.append(name2, uid, [{11, 1, {:cmd, 11}}])
    GenServer.stop(pid2)

    assert {6, 11, recovered2} = LogWal.recover(%{data_dir: server_dir})
    assert Enum.map(recovered2, &elem(&1, 0)) == Enum.to_list(6..11)
  end

  test "suffix truncation removes conflicting entries from disk" do
    base = RaftEx.TestSupport.tmp_dir("wal_truncate_suffix")
    uid = "trunc-suffix-uid"
    {name, pid} = start_wal(base)

    for i <- 1..10 do
      assert {:ok, _, _} = LogWal.append(name, uid, [{i, 1, {:cmd, i}}])
    end

    path = Path.join([base, to_string(uid), "wal.dat"])
    {:ok, before_size} = File.stat(path) |> then(fn {:ok, s} -> {:ok, s.size} end)

    assert :ok = LogWal.truncate_from(name, uid, 6)

    {:ok, after_size} = File.stat(path) |> then(fn {:ok, s} -> {:ok, s.size} end)
    assert after_size < before_size

    GenServer.stop(pid)
    server_dir = Path.join(base, to_string(uid))

    {_first, last, recovered} = LogWal.recover(%{data_dir: server_dir})
    assert last == 5
    assert Enum.map(recovered, &elem(&1, 0)) == Enum.to_list(1..5)
  end
end
