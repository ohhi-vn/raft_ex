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

    test "begin_read and read_chunk" do
      %{dir: dir} = self_dir()
      LogSnapshot.write(dir, %{index: 1, term: 1}, :state, true)

      assert {:ok, meta, read_state} = LogSnapshot.begin_read(dir, LogSnapshot.context())
      assert meta == %{index: 1, term: 1}

      {:ok, _data, _next} = LogSnapshot.read_chunk(read_state, 128, dir)
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
  end

  defp self_dir, do: %{dir: Process.get(:log_snapshot_test_dir)}
end
