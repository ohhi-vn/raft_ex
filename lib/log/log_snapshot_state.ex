defmodule RaftEx.LogSnapshotState do
  @table :ra_log_snapshot_state

  def insert(table \\ @table, uid, snap_idx, smallest_idx, live_indexes)
      when is_binary(uid) and is_integer(snap_idx) and is_integer(smallest_idx) and
             is_list(live_indexes) do
    true = :ets.insert(table, {uid, snap_idx, smallest_idx, live_indexes})
    :ok
  end

  def delete(table \\ @table, uid) do
    true = :ets.delete(table, uid)
    :ok
  end

  def smallest(table \\ @table, uid) when is_binary(uid) do
    :ets.lookup_element(table, uid, 3, 0)
  end

  def live_indexes(table \\ @table, uid) when is_binary(uid) do
    :ets.lookup_element(table, uid, 4, [])
  end

  def snapshot(table \\ @table, uid) when is_binary(uid) do
    :ets.lookup_element(table, uid, 2, -1)
  end

  def read(table \\ @table, uid) when is_binary(uid) do
    case :ets.lookup(table, uid) do
      [] -> nil
      [record] -> record
    end
  end
end
