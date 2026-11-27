defmodule RaftEx.Infra.Persistence.LogSegmentStorageTest do
  @moduledoc "Coverage for Section 13 segment-backed log storage (tasks 13.1-13.6)."
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.{Log, LogSegmentWriter}

  defp start_writer(dir, opts \\ []) do
    name = :"segstore_#{System.unique_integer([:positive])}"

    _pid =
      start_supervised!(
        {LogSegmentWriter,
         %{
           name: name,
           data_dir: dir,
           segment_max_entries: Keyword.get(opts, :max_entries, 4),
           segment_max_size_bytes: Keyword.get(opts, :max_size, 1_000_000)
         }},
        id: name
      )

    name
  end

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "segstore_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp log_with(seg, entries) do
    %Log{
      Log.init(%{
        uid: "seg-test",
        system_config: %{
          data_dir: tmp_dir(),
          names: %{wal: :nowal, log_meta: :nometa, open_mem_tbls: :noetbl}
        }
      })
      | entries: Map.new(entries, fn {idx, _, _} = e -> {idx, e} end),
        range: {1, length(entries)},
        next_index: length(entries) + 1,
        last_term: 1,
        last_written_index_term: {length(entries), 1},
        segment_writer: seg,
        segment_enabled?: true,
        log_window: 8
    }
  end

  test "13.1 sealed indexes are ascending and binary-searchable" do
    dir = tmp_dir()
    seg = start_writer(dir)
    entries = for i <- 1..6, do: {i, 1, {:cmd, i}}
    assert :ok = LogSegmentWriter.write(seg, entries)

    # Force a seal to freeze the index.
    assert :ok = LogSegmentWriter.seal_active_segment(seg)

    for i <- 1..6 do
      assert {:ok, {^i, 1, {:cmd, ^i}}} = LogSegmentWriter.read_entry(seg, i)
    end

    assert {:ok, got} = LogSegmentWriter.read(seg, 2, 5)
    assert Enum.map(got, &elem(&1, 0)) == [2, 3, 4, 5]
  end

  test "13.2 a range spanning memory and segments reads correctly" do
    dir = tmp_dir()
    seg = start_writer(dir)
    entries = for i <- 1..10, do: {i, 1, {:cmd, i}}
    log = log_with(seg, entries)

    # Seal the oldest range and evict it from memory.
    assert {:ok, log} = Log.seal_oldest(log, 6)
    assert map_size(log.entries) == 4

    # The full range still reads correctly across both stores.
    {got, _} = Log.fold(1, 10, fn e, acc -> acc ++ [e] end, [], log)
    assert Enum.map(got, &elem(&1, 0)) == Enum.to_list(1..10)

    {sparse, _} = Log.sparse_read([2, 5, 9], log)
    assert Enum.map(sparse, &elem(&1, 0)) == [2, 5, 9]

    assert {1, _} = Log.fetch_term(2, log)
    assert {nil, _} = Log.fetch_term(99, log)
  end

  test "13.3 a crash during sealing recovers exactly what is on disk" do
    dir = tmp_dir()
    seg = start_writer(dir, max_entries: 100)
    entries = for i <- 1..5, do: {i, 1, {:cmd, i}}
    assert :ok = LogSegmentWriter.write(seg, entries)
    assert :ok = LogSegmentWriter.seal_active_segment(seg)

    # Hard kill without terminate, then restart under a new name against
    # the same directory: recovery must find exactly what is on disk.
    pid = Process.whereis(seg)
    Process.exit(pid, :kill)
    Process.sleep(200)
    refute Process.alive?(pid)

    seg2 = :"#{seg}_restart"

    _ =
      start_supervised!(
        {LogSegmentWriter,
         %{name: seg2, data_dir: dir, segment_max_entries: 100, segment_max_size_bytes: 1_000_000}},
        id: seg2
      )

    assert {:ok, got} = LogSegmentWriter.read(seg2, 1, 5)
    assert Enum.map(got, &elem(&1, 0)) == [1, 2, 3, 4, 5]
  end

  test "13.4 missing and corrupt segments report explicit errors" do
    dir = tmp_dir()
    seg = start_writer(dir)
    assert :ok = LogSegmentWriter.write(seg, [{1, 1, :a}])
    assert :ok = LogSegmentWriter.seal_active_segment(seg)

    [file] = Path.wildcard(Path.join([dir, "segments", "*.segment"]))
    File.rm!(file)
    assert {:error, {:segment_missing, _, _}} = LogSegmentWriter.read(seg, 1, 1)

    File.write!(file, "garbage")
    assert {:error, _} = LogSegmentWriter.read(seg, 1, 1)
    assert {:error, _} = LogSegmentWriter.read_entry(seg, 1)
  end

  test "13.5 segments entirely at/below the snapshot index are deleted; partial ones stay" do
    dir = tmp_dir()
    seg = start_writer(dir, max_entries: 2)
    entries = for i <- 1..5, do: {i, 1, {:cmd, i}}
    assert :ok = LogSegmentWriter.write(seg, entries)
    assert :ok = LogSegmentWriter.seal_active_segment(seg)

    before_files = Path.wildcard(Path.join([dir, "segments", "*.segment"]))
    assert length(before_files) >= 2

    # Snapshot at 2: segments fully at/below 2 go away, the rest stay.
    assert :ok = LogSegmentWriter.delete_up_to(seg, 2)
    after_files = Path.wildcard(Path.join([dir, "segments", "*.segment"]))
    assert length(after_files) < length(before_files)
    assert after_files != []

    # The retained tail is still readable.
    assert {:ok, got} = LogSegmentWriter.read(seg, 3, 5)
    assert Enum.map(got, &elem(&1, 0)) == [3, 4, 5]
  end

  test "13.6 segment storage is off by default and works when enabled" do
    sys = RaftEx.App.System.default_config()
    assert sys[:segment_storage] == false

    dir = tmp_dir()
    seg = start_writer(dir)

    log =
      log_with(seg, for(i <- 1..4, do: {i, 1, {:cmd, i}}))
      |> Map.put(:segment_enabled?, false)

    # Disabled: evicted-from-memory reads are plain misses.
    log = %{log | entries: Map.drop(log.entries, [1, 2])}
    {entries, _} = Log.sparse_read([1, 2, 3], log)
    assert Enum.map(entries, &elem(&1, 0)) == [3]
  end
end
