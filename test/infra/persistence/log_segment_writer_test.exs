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

  defp name, do: Process.get(:segment_writer_test_name)
end
