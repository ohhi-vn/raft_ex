defmodule RaftEx.Infra.Persistence.LogTest do
  use ExUnit.Case, async: true

  alias RaftEx.Infra.Persistence.{Log, LogMeta, LogSnapshot, LogWal}

  defmodule FailingWal do
    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)

    @impl GenServer
    def init(:ok), do: {:ok, :ok}

    @impl GenServer
    def handle_call({:append, _uid, _entries}, _from, state) do
      {:reply, {:error, :wal_write_failed}, state}
    end
  end

  defmodule FailingSnapshot do
    def write(_dir, _meta, _state, _sync), do: {:error, :snapshot_failed}
  end

  defp log(entries \\ []) do
    entries_map = Map.new(entries, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)
    indexes = Enum.map(entries, &elem(&1, 0))

    {range, last_term} =
      case indexes do
        [] -> {nil, 0}
        _ -> {{Enum.min(indexes), Enum.max(indexes)}, entries |> List.last() |> elem(1)}
      end

    %Log{
      entries: entries_map,
      range: range,
      last_term: last_term,
      next_index: if(indexes == [], do: 0, else: Enum.max(indexes) + 1),
      last_written_index_term: {if(indexes == [], do: 0, else: Enum.max(indexes)), last_term},
      data_dir: "/tmp/log_test"
    }
  end

  describe "index and term queries" do
    test "last_index_term/1 handles logs with and without snapshots" do
      assert Log.last_index_term(log([{1, 2, :cmd}, {3, 2, :cmd}])) == {3, 2}
      assert Log.last_index_term(%Log{current_snapshot: nil}) == {0, 0}
      assert Log.last_index_term(%Log{current_snapshot: {5, 3}}) == {5, 3}
    end

    test "last_written/1 and set_last_index/2" do
      state = log()
      assert Log.last_written(state) == {0, 0}
      assert {:ok, updated} = Log.set_last_index(4, state)
      assert updated.range == {4, 4}
    end

    test "next_index/1 covers all representations" do
      assert Log.next_index(%Log{next_index: 5}) == 5
      assert Log.next_index(%Log{range: {1, 7}}) == 8
      assert Log.next_index(%Log{current_snapshot: {9, 1}}) == 10
      assert Log.next_index(%Log{current_snapshot: nil}) == 0
    end

    test "fetch_term/2 and exists/2" do
      state = log([{1, 2, :cmd}])
      assert Log.fetch_term(1, state) == {2, state}
      assert Log.fetch_term(2, state) == {nil, state}
      assert {true, _} = Log.exists({1, 2}, state)
      assert {false, _} = Log.exists({1, 3}, state)
    end
  end

  describe "snapshot helpers" do
    test "snapshot_state, set_snapshot_state, snapshot_size" do
      state = log()
      assert Log.snapshot_state(state) == nil
      assert is_nil(Log.snapshot_size(state)) or is_integer(Log.snapshot_size(state))
      updated = Log.set_snapshot_state(:snap, state)
      assert Log.snapshot_state(updated) == :snap
    end

    test "checkpoint writes a snapshot and compacts the log" do
      dir = RaftEx.TestSupport.tmp_dir("log_checkpoint_test")

      state = %{
        log([{1, 1, :a}, {2, 2, :b}])
        | data_dir: dir,
          cfg: %{uid: "checkpoint-uid", snapshot_module: RaftEx.Infra.Persistence.LogSnapshot}
      }

      {updated, effects} =
        Log.checkpoint(2, %{}, {0, RaftEx.Domain.Machine.Simple}, :mac_state, state)

      assert effects == []
      assert updated.current_snapshot == {2, 2}
      assert map_size(updated.entries) == 0
      assert updated.next_index == 3

      assert {:ok, %{index: 2, term: 2}, :mac_state} =
               LogSnapshot.recover(Path.join(dir, "snapshot"))
    end

    test "install_snapshot removes covered entries and sets the snapshot" do
      state = log([{1, 2, :cmd}, {2, 2, :cmd}, {3, 2, :cmd}])

      {:ok, updated, effects} = Log.install_snapshot({2, 2}, nil, [], state)

      assert map_size(updated.entries) == 1
      assert updated.current_snapshot == {2, 2}
      assert updated.next_index == 3
      assert effects == []
      assert Log.recover_snapshot(updated) == nil
      assert Log.snapshot_index_term(updated) == {2, 2}
    end
  end

  describe "cursor and maintenance helpers" do
    test "tick updates the last resend time" do
      assert Log.tick(1234, log()).last_resend_time == 1234
    end

    test "update_release_cursor and checkpoint keep the log" do
      state = log([{1, 2, :cmd}])
      assert {^state, []} = Log.update_release_cursor(1, %{}, {0, nil}, :mac, state)
      assert {^state, []} = Log.checkpoint(1, %{}, {0, nil}, :mac, state)
    end

    test "truncate removes entries from a given index" do
      state = log([{1, 2, :cmd}, {2, 2, :cmd}, {3, 2, :cmd}])
      truncated = Log.truncate(2, state)

      assert Map.keys(truncated.entries) == [1]
      assert truncated.range == {1, 1}
    end

    test "truncate drops the conflicting suffix and clears the mem table" do
      table = :"truncate_mem_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :named_table, :public])
      :ets.insert(table, [{1, 1, :a}, {2, 2, :b}, {3, 3, :c}])

      state = %{
        log([{1, 1, :a}, {2, 2, :b}, {3, 3, :c}])
        | mem_table: table,
          cfg: %{uid: "truncate_conflict"},
          wal_name: :"missing_wal_#{System.unique_integer([:positive])}"
      }

      truncated = Log.truncate(2, state)

      assert Map.keys(truncated.entries) == [1]
      assert truncated.range == {1, 1}
      assert truncated.last_term == 1
      assert truncated.last_written_index_term == {1, 1}
      assert truncated.next_index == 2
      assert :ets.tab2list(table) == [{1, 1, :a}]

      :ets.delete(table)
    end

    test "truncate probes past gaps in a sparse log" do
      state = log([{1, 1, :a}, {5, 5, :e}])

      truncated = Log.truncate(3, state)

      assert Map.keys(truncated.entries) == [1]
      assert truncated.range == {1, 1}
      assert truncated.last_term == 1
    end

    test "can_write?, has_pending? and overview" do
      state = log()
      assert Log.can_write?(state)
      refute Log.has_pending?(state)
      assert Log.has_pending?(%{state | pending: [:p]})

      overview = Log.overview(state)
      assert overview.type == Log
      assert overview.entries_count == 0
      assert overview.pending_count == 0
    end

    test "handle_event is a no-op" do
      state = log()
      assert Log.handle_event(:anything, state) == {state, []}
    end

    test "release_resources is a no-op" do
      state = log()
      assert Log.release_resources(10, :lru, state) == state
    end
  end

  describe "reads" do
    test "append adds an entry" do
      updated = Log.append({1, 2, :cmd}, log())

      assert updated.entries[1] == {1, 2, :cmd}
      assert updated.last_term == 2
      assert updated.next_index == 2
    end

    test "write batches the mem-table insert and merges entries in one pass" do
      table = :"write_mem_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :named_table, :public])

      state = %{
        log([{1, 1, :a}])
        | mem_table: table,
          cfg: %{uid: "batch_write"},
          wal_name: nil
      }

      assert {:ok, updated} = Log.write([{2, 2, :b}, {3, 3, :c}], state)

      assert updated.entries |> Map.keys() |> Enum.sort() == [1, 2, 3]
      assert updated.range == {1, 3}
      assert updated.last_term == 3
      assert updated.last_written_index_term == {3, 3}
      assert updated.next_index == 4
      assert :ets.tab2list(table) |> Enum.sort() == [{2, 2, :b}, {3, 3, :c}]

      :ets.delete(table)
    end

    test "fold reads an inclusive range" do
      state = log([{1, 2, :one}, {2, 2, :two}, {3, 2, :three}])

      {entries, _state} = Log.fold(1, 2, fn entry, acc -> [entry | acc] end, [], state)
      assert entries == [{2, 2, :two}, {1, 2, :one}]
    end

    test "fold outside the range is a no-op" do
      state = log([{1, 2, :one}])
      assert Log.fold(5, 7, fn e, a -> [e | a] end, [], state) == {[], state}
    end

    test "fold reads a dense range by probing indexes" do
      entries = Enum.map(1..100, fn idx -> {idx, 1, {:cmd, idx}} end)
      state = log(entries)

      {folded, _} =
        Log.fold(40, 42, fn e, a -> [e | a] end, [], state)

      assert Enum.reverse(folded) == [
               {40, 1, {:cmd, 40}},
               {41, 1, {:cmd, 41}},
               {42, 1, {:cmd, 42}}
             ]
    end

    test "fold skips gaps in a sparse log" do
      state = log([{1, 1, :a}, {3, 1, :c}, {10, 1, :j}])

      {folded, _} = Log.fold(1, 10, fn e, a -> [e | a] end, [], state)

      assert Enum.reverse(folded) == [{1, 1, :a}, {3, 1, :c}, {10, 1, :j}]
    end

    test "fold handles a range larger than the log" do
      state = log([{1, 1, :a}, {2, 1, :b}])

      {folded, _} = Log.fold(1, 1_000_000, fn e, a -> [e | a] end, [], state)

      assert Enum.reverse(folded) == [{1, 1, :a}, {2, 1, :b}]
    end

    test "sparse_read returns only present indexes" do
      state = log([{1, 2, :one}, {3, 2, :three}])
      {entries, _state} = Log.sparse_read([1, 2, 3], state)
      assert entries == [{1, 2, :one}, {3, 2, :three}]
    end

    test "partial_read and execute_read_plan" do
      state = log([{1, 2, :one}])
      plan = Log.partial_read([1, 2], state, fn _i, _t, cmd -> cmd end)

      assert plan.dir == "/tmp/log_test"
      assert plan.read == %{1 => {1, 2, :one}}
      assert plan.plan == [{:read_from_segment, 2}]

      assert Log.read_plan_info(plan) == %{num_read: 1, num_in_segments: 1, num_segments: 0}

      {results, flru} = Log.execute_read_plan(plan, nil, fn _i, _t, cmd -> cmd end, [])
      assert results == %{1 => :one}
      assert %RaftEx.Domain.LruCache{} = flru
    end

    test "fold with the :return strategy delegates to the plain fold" do
      state = log([{1, 2, :one}, {2, 2, :two}])

      {entries, ^state} =
        Log.fold(1, 2, fn entry, acc -> [entry | acc] end, [], state, :return)

      assert entries == [{2, 2, :two}, {1, 2, :one}]
    end

    test "execute_read_plan maps entries that do not match an index to nil" do
      plan = %{read: %{1 => :not_an_entry}, plan: []}

      {results, _flru} = Log.execute_read_plan(plan, nil, fn _i, _t, _c -> :ok end, [])
      assert results == %{1 => nil}
    end

    test "snapshot_size returns nil when no snapshot has been written" do
      dir = RaftEx.TestSupport.tmp_dir("log_snapshot_size")

      assert Log.snapshot_size(%{log() | data_dir: dir}) == nil
    end
  end

  describe "initialization and recovery" do
    defp start_meta(data_dir) do
      suffix = System.unique_integer([:positive])
      system = :"log_init_system_#{suffix}"
      meta_name = :"#{system}_meta"

      start_supervised!(%{
        id: meta_name,
        start:
          {LogMeta, :start_link,
           [%{name: system, data_dir: data_dir, names: %{log_meta: meta_name}}]}
      })

      meta_name
    end

    defp init_config(uid, data_dir, meta_name) do
      %{
        uid: uid,
        system_config: %{
          data_dir: data_dir,
          names: %{wal: :unused_wal, log_meta: meta_name, open_mem_tbls: :unused_mem_tbls}
        }
      }
    end

    test "init derives the last term from a snapshot when the WAL is empty" do
      uid = "log_init_snapshot_#{System.unique_integer([:positive])}"
      data_dir = RaftEx.TestSupport.tmp_dir("log_init_snapshot")
      meta_name = start_meta(data_dir)
      snap_dir = Path.join([data_dir, uid, "snapshot"])
      File.mkdir_p!(snap_dir)
      assert {:ok, _bytes} = LogSnapshot.write(snap_dir, %{index: 4, term: 7}, :snap_state, true)

      state = Log.init(init_config(uid, data_dir, meta_name))

      assert state.last_term == 7
      assert state.current_snapshot == {4, 7}
      assert state.range == {4, 4}
      assert state.next_index == 5
      assert state.snapshot_state == :snap_state
      Log.close(state)
    end

    test "init takes the last term from the highest WAL index" do
      uid = "log_init_wal_term_#{System.unique_integer([:positive])}"
      data_dir = RaftEx.TestSupport.tmp_dir("log_init_wal_term")
      meta_name = start_meta(data_dir)
      wal_name = :"log_init_wal_#{System.unique_integer([:positive])}"

      start_supervised!(%{
        id: wal_name,
        start:
          {LogWal, :start_link,
           [%{names: %{wal: wal_name}, data_dir: data_dir, wal_compute_checksums: true}]}
      })

      entries = [{1, 2, :a}, {2, 5, :b}, {3, 9, :c}]
      assert {:ok, 1, 3} = LogWal.append(wal_name, uid, entries)

      state = Log.init(init_config(uid, data_dir, meta_name))

      assert state.last_term == 9
      assert state.range == {1, 3}
      assert state.next_index == 4
      assert state.entries[3] == {3, 9, :c}
      Log.close(state)
    end

    test "init ignores a corrupt snapshot file" do
      uid = "log_init_corrupt_#{System.unique_integer([:positive])}"
      data_dir = RaftEx.TestSupport.tmp_dir("log_init_corrupt_snapshot")
      meta_name = start_meta(data_dir)
      snap_dir = Path.join([data_dir, uid, "snapshot"])
      File.mkdir_p!(snap_dir)
      File.write!(Path.join(snap_dir, "snapshot.dat"), "garbage")

      state = Log.init(init_config(uid, data_dir, meta_name))

      assert state.current_snapshot == nil
      assert state.snapshot_state == nil
      assert state.range == nil
      assert state.last_term == 0
      Log.close(state)
    end
  end

  describe "failure paths" do
    test "write reports a WAL failure without changing the log" do
      name = :"failing_wal_#{System.unique_integer([:positive])}"
      start_supervised!(%{id: name, start: {FailingWal, :start_link, [name]}})

      state = %{
        log()
        | cfg: %{uid: "failing_wal_uid", system_config: %{data_dir: "/tmp"}},
          wal_name: name
      }

      assert {:error, :wal_write_failed} = Log.write([{1, 1, :a}], state)
    end

    test "checkpoint keeps the remaining entries and ignores a missing WAL" do
      dir = RaftEx.TestSupport.tmp_dir("log_checkpoint_remaining")

      state = %{
        log([{1, 1, :a}, {2, 2, :b}, {3, 3, :c}])
        | data_dir: dir,
          cfg: %{uid: "checkpoint_remaining", snapshot_module: LogSnapshot},
          wal_name: :"missing_wal_#{System.unique_integer([:positive])}"
      }

      {updated, []} = Log.checkpoint(2, %{}, {0, RaftEx.Domain.Machine.Simple}, :mac, state)

      assert map_size(updated.entries) == 1
      assert updated.range == {3, 3}
      assert updated.last_term == 3
      assert updated.last_written_index_term == {3, 3}
      assert updated.next_index == 4
    end

    test "checkpoint leaves the log alone when the snapshot module fails" do
      state = %{
        log([{1, 1, :a}])
        | data_dir: RaftEx.TestSupport.tmp_dir("log_checkpoint_error"),
          cfg: %{uid: "failing_snapshot", snapshot_module: FailingSnapshot}
      }

      assert {^state, []} = Log.checkpoint(1, %{}, {0, nil}, :mac, state)
    end

    test "truncate ignores a missing WAL process" do
      state = %{
        log([{1, 1, :a}, {2, 1, :b}])
        | cfg: %{uid: "truncate_missing_wal"},
          wal_name: :"missing_wal_#{System.unique_integer([:positive])}"
      }

      truncated = Log.truncate(2, state)

      assert Map.keys(truncated.entries) == [1]
      assert truncated.range == {1, 1}
      assert truncated.last_written_index_term == {1, 1}
      assert truncated.next_index == 2
    end
  end

  describe "configuration and cleanup" do
    test "read_config falls back to evaluating legacy term syntax" do
      dir = RaftEx.TestSupport.tmp_dir("log_legacy_config")
      path = Path.join(dir, "config")

      File.write!(path, "%{a: 1}")
      assert {:ok, %{a: 1}} = Log.read_config(dir)

      File.write!(path, "{")
      assert {:error, _reason} = Log.read_config(dir)
    end

    test "delete_everything closes the mem table and removes the data directory" do
      dir = RaftEx.TestSupport.tmp_dir("log_delete_everything")
      uid = "delete_everything_uid_#{System.unique_integer([:positive])}"
      subdir = Path.join(dir, uid)
      File.mkdir_p!(subdir)
      File.write!(Path.join(subdir, "marker"), "x")

      table = :"delete_mem_table_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :named_table, :public])

      state = %{log() | data_dir: dir, cfg: %{uid: uid}, mem_table: table}

      assert :ok = Log.delete_everything(state)
      refute File.exists?(subdir)
      assert :ets.info(table) == :undefined
    end
  end
end
