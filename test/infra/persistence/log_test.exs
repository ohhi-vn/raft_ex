defmodule RaftEx.Infra.Persistence.LogTest do
  use ExUnit.Case, async: true

  alias RaftEx.Infra.Persistence.Log

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
      assert Log.snapshot_size(state) == nil
      updated = Log.set_snapshot_state(:snap, state)
      assert Log.snapshot_state(updated) == :snap
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

    test "fold reads an inclusive range" do
      state = log([{1, 2, :one}, {2, 2, :two}, {3, 2, :three}])

      {entries, _state} = Log.fold(1, 2, fn entry, acc -> [entry | acc] end, [], state)
      assert entries == [{2, 2, :two}, {1, 2, :one}]
    end

    test "fold outside the range is a no-op" do
      state = log([{1, 2, :one}])
      assert Log.fold(5, 7, fn e, a -> [e | a] end, [], state) == {[], state}
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
  end
end
