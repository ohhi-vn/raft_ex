defmodule RaftEx.Infra.Persistence.LogSegmentWriterTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogSegmentWriter

  setup do
    data_dir = RaftEx.TestSupport.tmp_dir("log_segment_writer_test")
    name = :"segment_writer_test_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogSegmentWriter,
       %{
         name: name,
         data_dir: data_dir,
         segment_max_entries: 2,
         segment_max_size_bytes: 1_000_000
       }}
    )

    Process.put(:segment_writer_test_name, name)
    Process.put(:segment_writer_test_dir, data_dir)
    :ok
  end

  test "recover reports no segments in a fresh directory" do
    assert {:ok, _segments} =
             LogSegmentWriter.recover(%{data_dir: Process.get(:segment_writer_test_dir)})
  end

  test "write, read and overview" do
    writer = name()
    assert :ok = LogSegmentWriter.write(writer, [])
    assert :ok = LogSegmentWriter.write(writer, [{1, 1, :a}, {2, 1, :b}])

    assert {:ok, [{1, 1, :a}, {2, 1, :b}]} = LogSegmentWriter.read(writer, 1, 2)
    assert {:ok, {1, 1, :a}} = LogSegmentWriter.read_entry(writer, 1)
    assert {:error, :not_found} = LogSegmentWriter.read_entry(writer, 99)
    assert {1, 2} = LogSegmentWriter.index_range(writer)
    assert is_map(LogSegmentWriter.overview(writer))
  end

  test "rotation on entry limit" do
    writer = name()
    LogSegmentWriter.write(writer, [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}])

    assert {:ok, [{3, 1, :c}]} = LogSegmentWriter.read(writer, 3, 3)
    assert {1, 4} = LogSegmentWriter.index_range(writer)
  end

  test "seal_active_segment, truncate_from and delete_up_to" do
    writer = name()
    LogSegmentWriter.write(writer, [{1, 1, :a}, {2, 1, :b}])

    assert :ok = LogSegmentWriter.seal_active_segment(writer)
    assert :ok = LogSegmentWriter.truncate_from(writer, 2)
    assert :ok = LogSegmentWriter.delete_up_to(writer, 1)
  end

  describe "recovery errors" do
    test "recover reports an error when the segment path is not a directory" do
      dir = RaftEx.TestSupport.tmp_dir("segment_recover_error")
      File.write!(Path.join(dir, "segments"), "not a directory")

      assert {:error, _reason} = LogSegmentWriter.recover(%{data_dir: dir})
    end

    test "init treats a recovery error as an empty segment list" do
      dir = RaftEx.TestSupport.tmp_dir("segment_init_error")
      File.write!(Path.join(dir, "segments"), "not a directory")

      assert_raise File.Error, fn ->
        LogSegmentWriter.init(%{name: :unused_segment_writer, data_dir: dir})
      end
    end

    test "corrupt segment files are skipped" do
      dir = RaftEx.TestSupport.tmp_dir("segment_corrupt")
      segment_dir = Path.join(dir, "segments")
      File.mkdir_p!(segment_dir)
      File.write!(Path.join(segment_dir, "1-1.segment"), "not a segment")

      assert {:ok, []} = LogSegmentWriter.recover(%{data_dir: dir})
    end
  end

  describe "segment lifecycle edge cases" do
    test "a sealed writer creates a new active segment and old segments stay readable" do
      writer = name()
      assert :ok = LogSegmentWriter.write(writer, [{1, 1, :a}, {2, 1, :b}])

      assert :ok = LogSegmentWriter.seal_active_segment(writer)
      assert :ok = LogSegmentWriter.seal_active_segment(writer)

      assert :ok = LogSegmentWriter.write(writer, [{3, 1, :c}])

      assert {:ok, {1, 1, :a}} = LogSegmentWriter.read_entry(writer, 1)
      assert {:ok, {3, 1, :c}} = LogSegmentWriter.read_entry(writer, 3)
    end

    test "an empty store has a zero range and restarts numbering at one" do
      writer = name()
      LogSegmentWriter.write(writer, [{1, 1, :a}])
      assert :ok = LogSegmentWriter.seal_active_segment(writer)
      assert :ok = LogSegmentWriter.delete_up_to(writer, 1)

      assert {0, 0} = LogSegmentWriter.index_range(writer)
      assert :ok = LogSegmentWriter.write(writer, [{1, 1, :a}])
      assert {1, 1} = LogSegmentWriter.index_range(writer)
    end

    test "truncate_from discards the active segment when it starts at the cutoff" do
      writer = name()
      LogSegmentWriter.write(writer, [{1, 1, :a}])

      assert :ok = LogSegmentWriter.truncate_from(writer, 1)
      assert {0, 0} = LogSegmentWriter.index_range(writer)
    end

    test "unexpected casts and info messages are ignored" do
      writer = name()
      GenServer.cast(writer, :ignored_cast)
      send(writer, :ignored_info)

      assert %{num_segments: _} = LogSegmentWriter.overview(writer)
      assert is_pid(Process.whereis(writer))
    end

    test "write returns an error when the segment file cannot be opened" do
      writer = name()
      path = segment_path("1-1.segment")
      File.rm!(path)
      File.mkdir!(path)

      assert {:error, _reason} = LogSegmentWriter.write(writer, [{1, 1, :a}])
    end

    test "create_new_segment falls back to index one for an inconsistent range" do
      dir = RaftEx.TestSupport.tmp_dir("segment_inconsistent_range")
      {:ok, state} = LogSegmentWriter.init(%{name: :unused_writer, data_dir: dir})

      state = %{state | active_segment: nil, segments: [%{range: {2, 0}}]}

      assert {:reply, :ok, _state} =
               LogSegmentWriter.handle_call({:write, [{1, 1, :a}]}, nil, state)

      assert File.exists?(Path.join([dir, "segments", "1-1.segment"]))
    end
  end

  describe "unreadable segment data" do
    test "missing segment files are skipped when reading ranges and single entries" do
      writer = name()
      LogSegmentWriter.write(writer, [{1, 1, :a}])
      assert :ok = LogSegmentWriter.seal_active_segment(writer)
      File.rm!(segment_path("1-1.segment"))

      assert {:ok, []} = LogSegmentWriter.read(writer, 1, 1)
      assert {:error, :not_found} = LogSegmentWriter.read_entry(writer, 1)
    end

    test "segment files with an invalid header contain no entries" do
      writer = name()
      LogSegmentWriter.write(writer, [{1, 1, :a}])
      File.write!(segment_path("1-1.segment"), "garbage")

      assert {:ok, []} = LogSegmentWriter.read(writer, 1, 1)
      assert {:error, :not_found} = LogSegmentWriter.read_entry(writer, 1)
    end

    test "an entry missing from an in-range segment is not found" do
      writer = name()
      LogSegmentWriter.write(writer, [{1, 1, :a}])
      path = segment_path("1-1.segment")
      <<prefix::binary-size(8), first::64, _last::64, count::32, rest::binary>> = File.read!(path)
      File.write!(path, [prefix, <<first::64, 5::64, count::32>>, rest])

      dir = Process.get(:segment_writer_test_dir)
      recovered = LogSegmentWriter.recover(%{data_dir: dir}) |> elem(1)
      state = %{active_segment: nil, segments: recovered}

      assert {:reply, {:error, :not_found}, _state} =
               LogSegmentWriter.handle_call({:read_entry, 5}, nil, state)
    end

    test "delete_up_to survives a segment path that cannot be removed" do
      writer = name()
      LogSegmentWriter.write(writer, [{1, 1, :a}])
      assert :ok = LogSegmentWriter.seal_active_segment(writer)

      path = segment_path("1-1.segment")
      File.rm!(path)
      File.mkdir!(path)
      File.write!(Path.join(path, "keep"), "x")

      assert :ok = LogSegmentWriter.delete_up_to(writer, 1)
    end
  end

  defp name, do: Process.get(:segment_writer_test_name)

  defp segment_path(file) do
    Path.join([Process.get(:segment_writer_test_dir), "segments", file])
  end
end
