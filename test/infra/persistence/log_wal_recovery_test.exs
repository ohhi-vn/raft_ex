defmodule RaftEx.Infra.Persistence.LogWalRecoveryTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogWal

  defp start_wal(base) do
    name = :"recovery_wal_#{System.unique_integer([:positive])}"
    {:ok, pid} = LogWal.start_link(%{names: %{wal: name}, data_dir: base})
    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {name, pid}
  end

  defp wal_path(base, uid), do: Path.join([base, to_string(uid), "wal.dat"])

  defp append_n(name, uid, n) do
    entries = for i <- 1..n, do: {i, 1, {:cmd, i}}

    for e <- entries do
      assert {:ok, _, _} = LogWal.append(name, uid, [e])
    end

    entries
  end

  test "CRC corruption stops recovery at the last valid record" do
    base = RaftEx.TestSupport.tmp_dir("wal_crc")
    uid = "crc-uid"
    {name, pid} = start_wal(base)
    append_n(name, uid, 5)
    GenServer.stop(pid)

    path = wal_path(base, uid)
    # First record: 13-byte header, then the blob. Flip a blob byte so the
    # stored CRC no longer matches.
    assert :ok = RaftEx.TestSupport.WalFault.corrupt_byte(path, 15)

    {_first, _last, recovered} = LogWal.recover(%{data_dir: Path.join(base, uid)})
    assert recovered == [], "corrupt first record must stop recovery, got #{inspect(recovered)}"
  end

  test "oversized record length stops recovery without crashing" do
    base = RaftEx.TestSupport.tmp_dir("wal_oversize")
    uid = "oversize-uid"
    {name, pid} = start_wal(base)
    append_n(name, uid, 3)
    GenServer.stop(pid)

    path = wal_path(base, uid)
    {:ok, bin} = File.read(path)
    # blob_size lives at bytes 9..12 of the first record header.
    <<prefix::binary-size(9), _old::32-unsigned, rest::binary>> = bin
    File.write!(path, <<prefix::binary, 0xFF, 0xFF, 0xFF, 0xFF, rest::binary>>)

    {_first, _last, recovered} = LogWal.recover(%{data_dir: Path.join(base, uid)})
    assert recovered == []
  end

  test "truncated header and torn tail recover the last valid prefix" do
    base = RaftEx.TestSupport.tmp_dir("wal_trunc")
    uid = "trunc-uid"
    {name, pid} = start_wal(base)
    append_n(name, uid, 5)
    GenServer.stop(pid)

    path = wal_path(base, uid)
    {:ok, bin} = File.read(path)
    assert byte_size(bin) > 20

    # Cut 10 bytes off the end: the last record is torn, earlier ones survive.
    assert :ok = RaftEx.TestSupport.WalFault.truncate_file(path, byte_size(bin) - 10)

    {_first, _last, recovered} = LogWal.recover(%{data_dir: Path.join(base, uid)})
    indexes = Enum.map(recovered, &elem(&1, 0))
    assert indexes == Enum.sort(indexes)
    assert length(indexes) < 5
    assert indexes != []

    # A file with only a partial magic header recovers nothing without crashing.
    File.write!(path, <<0x52, 0x41>>)
    assert {0, 0, []} = LogWal.recover(%{data_dir: Path.join(base, uid)})
  end

  test "empty payload does not crash recovery" do
    base = RaftEx.TestSupport.tmp_dir("wal_empty")
    uid = "empty-uid"
    {name, pid} = start_wal(base)
    append_n(name, uid, 2)
    GenServer.stop(pid)

    path = wal_path(base, uid)
    # Append a record header declaring a zero-length blob and no footer.
    File.write!(path, <<0x52, 0x41, 0x57, 0x4C, 2, 0, 0, 0, 1, 0, 0, 0, 0>>, [:append])
    {_first, _last, recovered} = LogWal.recover(%{data_dir: Path.join(base, uid)})
    indexes = Enum.map(recovered, &elem(&1, 0))
    assert indexes == [1, 2]
  end

  test "truncation at every byte boundary recovers a valid prefix without crashing" do
    base = RaftEx.TestSupport.tmp_dir("wal_bytes")
    uid = "bytes-uid"
    {name, pid} = start_wal(base)
    full = append_n(name, uid, 5)
    GenServer.stop(pid)

    path = wal_path(base, uid)
    {:ok, bin} = File.read(path)
    total = byte_size(bin)

    full_indexes = Enum.map(full, &elem(&1, 0))

    for cut <- 0..total do
      File.write!(path, binary_part(bin, 0, cut))
      {_first, _last, recovered} = LogWal.recover(%{data_dir: Path.join(base, uid)})
      indexes = Enum.map(recovered, &elem(&1, 0))

      # Recovered indexes are always a prefix of the written indexes, in order.
      assert indexes == Enum.take(full_indexes, length(indexes)),
             "cut at #{cut}/#{total}: expected prefix, got #{inspect(indexes)}"
    end
  end
end
