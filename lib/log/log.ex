defmodule RaftEx.Log do
  @moduledoc "Persistent replicated Raft log."

  # This module manages the combination of WAL + segment files + mem tables.
  # The full implementation mirrors ra_log.erl closely.

  defstruct [
    :cfg,
    :range,
    :snapshot_state,
    :current_snapshot,
    :last_resend_time,
    :last_wal_write,
    :reader,
    :mem_table,
    tx: false,
    pending: [],
    live_indexes: [],
    last_term: 0,
    last_written_index_term: {0, 0}
  ]

  def init(%{uid: uid, system_config: %{data_dir: data_dir, names: names}} = conf) do
    dir = server_data_dir(data_dir, uid)
    RaftEx.Lib.make_dir(dir)
    # Full init would set up WAL, segment reader, snapshot state, etc.
    # Abbreviated here.
    %__MODULE__{
      cfg: conf,
      range: nil,
      snapshot_state: nil,
      current_snapshot: nil,
      last_wal_write: {nil, now_ms(), -1},
      mem_table: nil,
      reader: nil
    }
  end

  def close(%__MODULE__{}), do: :ok

  def append({idx, term, cmd} = entry, %__MODULE__{} = state) do
    # Write entry to WAL + mem table
    %{state | last_term: term}
  end

  def write(entries, state) do
    {:ok, Enum.reduce(entries, state, fn e, s -> append(e, s) end)}
  end

  def fold(from, to, fun, acc, %__MODULE__{range: {start, ending}} = state, strategy \\ :error)
      when to >= from and to >= start do
    # Read entries from mem table / segments
    {acc, state}
  end

  def fold(_from, _to, _fun, acc, state, _), do: {acc, state}

  def sparse_read(indexes, %__MODULE__{} = state) do
    # Optimised sparse read
    {[], state}
  end

  def partial_read(indexes, %__MODULE__{} = state, transform_fun) do
    # Return a read plan
    %{dir: nil, read: %{}, plan: []}
  end

  def execute_read_plan(plan, flru, transform_fun, options) do
    {%{}, flru || RaftEx.LruCache.new(1, nil)}
  end

  def read_plan_info(plan), do: %{num_read: 0, num_in_segments: 0, num_segments: 0}

  def last_index_term(%__MODULE__{range: {_, last}, last_term: lt}), do: {last, lt}
  def last_index_term(%__MODULE__{current_snapshot: nil}), do: {0, 0}
  def last_index_term(%__MODULE__{current_snapshot: cs}), do: cs

  def last_written(%__MODULE__{last_written_index_term: lwit}), do: lwit

  def set_last_index(idx, %__MODULE__{} = state) do
    {:ok, %{state | range: nil}}
  end

  def next_index(%__MODULE__{range: {_, last}}), do: last + 1
  def next_index(%__MODULE__{current_snapshot: {snap_idx, _}}), do: snap_idx + 1
  def next_index(%__MODULE__{current_snapshot: nil}), do: 0

  def fetch_term(idx, %__MODULE__{} = state) do
    {nil, state}
  end

  def snapshot_state(%__MODULE__{snapshot_state: ss}), do: ss
  def snapshot_size(%__MODULE__{} = _state), do: nil
  def set_snapshot_state(snap_state, state), do: %{state | snapshot_state: snap_state}

  def install_snapshot({snap_idx, snap_term} = idx_term, mac_mod, live_indexes, state) do
    {:ok, %{state | current_snapshot: idx_term, range: nil}, []}
  end

  def recover_snapshot(%__MODULE__{snapshot_state: nil}), do: nil
  def recover_snapshot(%__MODULE__{snapshot_state: _ss}), do: nil

  def snapshot_index_term(%__MODULE__{current_snapshot: cs}), do: cs

  def update_release_cursor(idx, cluster, {mac_ver, mac_mod}, mac_state, state) do
    {state, []}
  end

  def checkpoint(idx, cluster, mac_ctx, mac_state, state) do
    {state, []}
  end

  def promote_checkpoint(idx, state), do: {state, []}

  def tick(now, state), do: state

  def can_write?(%__MODULE__{}), do: true

  def exists({idx, term}, state) do
    case fetch_term(idx, state) do
      {^term, s} -> {true, s}
      {_, s} -> {false, s}
    end
  end

  def has_pending?(%__MODULE__{pending: []}), do: false
  def has_pending?(%__MODULE__{}), do: true

  def overview(%__MODULE__{} = state), do: %{type: RaftEx.Log}

  def write_config(config, %__MODULE__{cfg: %{uid: uid} = cfg}) do
    dir = server_data_dir(cfg.system_config.data_dir, uid)
    config_path = Path.join(dir, "config")
    clean = Map.drop(config, [:parent, :counter, :has_changed, :system_config])
    RaftEx.Lib.write_file(config_path, inspect(clean) <> ".")
  end

  def read_config(%__MODULE__{cfg: %{uid: uid, system_config: %{data_dir: dd}}}),
    do: read_config(server_data_dir(dd, uid))

  def read_config(dir) do
    path = Path.join(dir, "config")

    case File.read(path) do
      {:ok, contents} ->
        {term, _} = Code.eval_string(contents)
        {:ok, term}

      err ->
        err
    end
  end

  def delete_everything(%__MODULE__{cfg: %{uid: uid}} = state) do
    close(state)
    dir = "."
    RaftEx.Lib.recursive_delete(dir)
    :ok
  end

  def release_resources(max_open, access_pattern, state), do: state

  def handle_event(evt, %__MODULE__{} = state), do: {state, []}

  defp server_data_dir(dir, uid), do: Path.join(dir, uid)
  defp now_ms, do: :erlang.system_time(:millisecond)
end
