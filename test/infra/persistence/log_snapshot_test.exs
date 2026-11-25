defmodule RaftEx.Infra.Persistence.LogSnapshotTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.{LogSnapshot, LogSnapshotState}

  setup do
    Process.put(:log_snapshot_test_dir, RaftEx.TestSupport.tmp_dir("log_snapshot_test"))
    :ok
  end

  defp chunk(meta) do
    meta_bin = :erlang.term_to_binary(meta)
    <<"RASN", 1, 0::32-integer, byte_size(meta_bin)::32-integer, meta_bin::binary>>
  end

  describe "write/read helpers" do
    test "prepare and context" do
      assert LogSnapshot.prepare(1, :state) == :state
      assert LogSnapshot.context() == %{can_accept_full_file: true}
    end

    test "write, read_meta, recover, validate and get_size" do
      %{dir: dir} = self_dir()
      meta = %{index: 3, term: 2}
      assert {:ok, _bytes} = LogSnapshot.write(dir, meta, :machine_state, true)

      assert {:ok, ^meta} = LogSnapshot.read_meta(dir)
      assert {:ok, _, _} = LogSnapshot.recover(dir)
      assert :ok = LogSnapshot.validate(dir)
      assert {:ok, size} = LogSnapshot.get_size(dir)
      assert size > 0
      assert :ok = LogSnapshot.sync(dir)
    end

    test "reading errors on an empty directory" do
      %{dir: dir} = self_dir()
      assert {:error, :enoent} = LogSnapshot.read_meta(dir)
      assert {:error, _} = LogSnapshot.recover(dir)
      assert {:error, _} = LogSnapshot.get_size(dir)
      assert {:error, _} = LogSnapshot.begin_read(dir, LogSnapshot.context())
    end

    test "recover rejects an invalid snapshot version" do
      %{dir: dir} = self_dir()
      File.write!(Path.join(dir, "snapshot.dat"), <<"RASN", 99, 0::32, "data">>)

      assert {:error, {:invalid_version, 99}} = LogSnapshot.recover(dir)
      assert {:error, {:invalid_version, 99}} = LogSnapshot.validate(dir)
    end
  end

  describe "chunked transfer" do
    test "begin_accept, accept_chunk, complete_accept and read_meta" do
      %{dir: dir} = self_dir()
      meta = %{index: 5, term: 3}
      assert {:ok, state} = LogSnapshot.begin_accept(dir, meta)

      assert {:ok, state} = LogSnapshot.accept_chunk(chunk(meta), state)
      assert {:ok, _bytes} = LogSnapshot.complete_accept(chunk(meta), state)

      assert {:ok, ^meta} = LogSnapshot.read_meta(dir)
    end

    test "accept_chunk returns an error when the file is no longer writable" do
      %{dir: dir} = self_dir()
      meta = %{index: 5, term: 3}
      assert {:ok, state} = LogSnapshot.begin_accept(dir, meta)

      {_bytes, _partial_crc, fd} = state
      :file.close(fd)

      assert {:error, _reason} = LogSnapshot.accept_chunk(chunk(meta), state)
    end

    test "accept_chunk rejects a malformed chunk" do
      %{dir: dir} = self_dir()
      assert {:ok, state} = LogSnapshot.begin_accept(dir, %{index: 5, term: 3})

      assert {:error, :invalid_chunk} = LogSnapshot.accept_chunk(<<1>>, state)
    end

    test "accept_chunk reports a write failure for a later chunk" do
      %{dir: dir} = self_dir()
      meta = %{index: 5, term: 3}
      {:ok, state} = LogSnapshot.begin_accept(dir, meta)
      {:ok, later_state} = LogSnapshot.accept_chunk(chunk(meta), state)

      {_bytes, _crc, _partial_crc, fd} = later_state
      :file.close(fd)

      assert {:error, _reason} = LogSnapshot.accept_chunk(<<1, 2, 3>>, later_state)
    end

    test "complete_accept reports a write failure instead of raising" do
      %{dir: dir} = self_dir()
      meta = %{index: 5, term: 3}
      {:ok, state} = LogSnapshot.begin_accept(dir, meta)
      {:ok, later_state} = LogSnapshot.accept_chunk(chunk(meta), state)

      {_bytes, _crc, _partial_crc, fd} = later_state
      :file.close(fd)

      assert {:error, _reason} = LogSnapshot.complete_accept(<<>>, later_state)
    end

    test "begin_read and read_chunk" do
      %{dir: dir} = self_dir()
      LogSnapshot.write(dir, %{index: 1, term: 1}, :state, true)

      assert {:ok, meta, read_state} = LogSnapshot.begin_read(dir, LogSnapshot.context())
      assert meta == %{index: 1, term: 1}

      {:ok, _data, _next} = LogSnapshot.read_chunk(read_state, 128, dir)
    end
  end

  describe "corrupt snapshots" do
    test "write returns filesystem errors" do
      %{dir: dir} = self_dir()
      File.mkdir!(Path.join(dir, "snapshot.dat"))

      assert {:error, _reason} = LogSnapshot.write(dir, %{index: 1, term: 1}, :state, true)
    end

    test "recover rejects a file with an unrecognised format" do
      %{dir: dir} = self_dir()
      File.write!(Path.join(dir, "snapshot.dat"), "XXXX")
      assert {:error, :invalid_format} = LogSnapshot.recover(dir)
    end

    test "recover rejects a checksum mismatch" do
      %{dir: dir} = self_dir()
      meta = %{index: 3, term: 2}
      assert {:ok, _bytes} = LogSnapshot.write(dir, meta, :state, true)

      path = Path.join(dir, "snapshot.dat")
      <<magic::binary-size(4), version, _crc::32, rest::binary>> = File.read!(path)
      File.write!(path, [magic, <<version>>, <<0::32>>, rest])

      assert {:error, :checksum_error} = LogSnapshot.recover(dir)
    end

    test "read_meta propagates metadata parsing errors" do
      %{dir: dir} = self_dir()
      File.write!(Path.join(dir, "snapshot.dat"), "bad")

      assert {:error, :invalid_format} = LogSnapshot.read_meta(dir)
    end

    test "read_meta reports a truncated metadata blob" do
      %{dir: dir} = self_dir()
      File.write!(Path.join(dir, "snapshot.dat"), <<"RASN", 1, 0::32, 100::32>>)
      assert LogSnapshot.read_meta(dir) == :eof
    end

    test "read_meta rejects an invalid version" do
      %{dir: dir} = self_dir()
      File.write!(Path.join(dir, "snapshot.dat"), <<"RASN", 99, 0::32, 0::32>>)

      assert {:error, {:invalid_version, 99}} = LogSnapshot.read_meta(dir)
      assert {:error, _} = LogSnapshot.begin_read(dir, LogSnapshot.context())
    end

    test "read_meta rejects a missing header" do
      %{dir: dir} = self_dir()
      File.write!(Path.join(dir, "snapshot.dat"), "")

      assert {:error, :unexpected_eof_when_parsing_header} = LogSnapshot.read_meta(dir)
    end
  end

  describe "chunked transfer errors" do
    test "read_chunk propagates read errors when a crc prefix is present" do
      %{dir: dir} = self_dir()

      assert {:error, :badarg} = LogSnapshot.read_chunk({123, {0, 100, :not_a_fd}}, 8, dir)
    end

    test "read_chunk reports an unexpected eof" do
      %{dir: dir} = self_dir()
      path = Path.join(dir, "snapshot.dat")
      File.write!(path, "")
      {:ok, fd} = :file.open(path, [:read, :binary, :raw])

      assert {:error, :unexpected_eof} = LogSnapshot.read_chunk({0, 1000, fd}, 10, dir)
      :file.close(fd)
    end
  end

  describe "LogSnapshotState" do
    test "insert, read and delete" do
      table = :"snap_state_test_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :named_table, :public])

      assert :ok = LogSnapshotState.insert(table, "uid", 5, 3, [3, 4, 5])
      assert LogSnapshotState.read(table, "uid") == {"uid", 5, 3, [3, 4, 5]}
      assert LogSnapshotState.snapshot(table, "uid") == 5
      assert LogSnapshotState.smallest(table, "uid") == 3
      assert LogSnapshotState.live_indexes(table, "uid") == [3, 4, 5]

      assert :ok = LogSnapshotState.delete(table, "uid")
      assert LogSnapshotState.read(table, "uid") == nil
      assert LogSnapshotState.snapshot(table, "uid") == -1
      assert LogSnapshotState.smallest(table, "uid") == 0
      assert LogSnapshotState.live_indexes(table, "uid") == []
    end

    test "the default snapshot state table can be used directly" do
      table = :ra_log_snapshot_state
      created = :ets.info(table) == :undefined
      if created, do: :ets.new(table, [:set, :named_table, :public])
      on_exit(fn -> if created, do: :ets.delete(table) end)

      assert :ok = LogSnapshotState.insert("default_uid", 9, 7, [7, 8, 9])
      assert LogSnapshotState.read("default_uid") == {"default_uid", 9, 7, [7, 8, 9]}
      assert LogSnapshotState.snapshot("default_uid") == 9
      assert LogSnapshotState.smallest("default_uid") == 7
      assert LogSnapshotState.live_indexes("default_uid") == [7, 8, 9]
      assert :ok = LogSnapshotState.delete("default_uid")
    end
  end

  defp self_dir, do: %{dir: Process.get(:log_snapshot_test_dir)}
end
