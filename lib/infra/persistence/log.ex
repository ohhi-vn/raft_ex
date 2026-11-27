defmodule RaftEx.Infra.Persistence.Log do
  @moduledoc """
  Persistent replicated Raft log.

  This module manages the combination of WAL + segment files + mem tables.
  It provides the core log operations needed by the Raft consensus algorithm:

  - **Append**: Add new entries to the log (via WAL)
  - **Read**: Fetch entries by index range or specific indexes
  - **Snapshot**: Install and recover snapshots
  - **Checkpoint**: Create recovery checkpoints for crash recovery
  - **Configuration**: Persist and read cluster configuration

  ## Architecture

  The log consists of several layers:
  1. **WAL (Write-Ahead Log)**: Durable, sequential write buffer
  2. **Mem Table**: In-memory buffer for recent entries
  3. **Segments**: Immutable on-disk files for older entries
  4. **Snapshot**: Point-in-time state with metadata

  ## Entry Format

  Each log entry is a tuple: `{index, term, command}`

  Where `command` can be:
  - `{:"$usr", metadata, user_command, reply_mode}` - User command
  - `{:"$ra_cluster_change", metadata, new_cluster, reply_mode}` - Membership change
  - `{:noop, metadata, next_mac_ver}` - No-op entry for leadership
  - `{:"$ra_leave", from, leaving, reply_mode}` - Leave command
  - `{:"$ra_cluster", metadata, :delete, reply_type}` - Delete cluster
  """

  require Logger

  alias RaftEx.Domain.LruCache
  alias RaftEx.Infra.Filesystem
  alias RaftEx.Infra.Persistence.LogMeta
  alias RaftEx.Infra.Persistence.LogSegmentWriter
  alias RaftEx.Infra.Persistence.LogSnapshot
  alias RaftEx.Infra.Persistence.LogWal

  # ---------------------------------------------------------------------------
  # State
  # ---------------------------------------------------------------------------

  defstruct [
    :cfg,
    :range,
    :snapshot_state,
    :snapshot_cluster,
    :current_snapshot,
    :last_resend_time,
    :last_wal_write,
    :reader,
    :mem_table,
    :wal_name,
    :meta_name,
    :data_dir,
    :segment_writer,
    tx: false,
    pending: [],
    live_indexes: [],
    last_term: 0,
    last_written_index_term: {0, 0},
    entries: %{},
    next_index: 0,
    segment_enabled?: false,
    log_window: 8192
  ]

  @type t :: %__MODULE__{
          cfg: map(),
          range: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index()} | nil,
          snapshot_state: term() | nil,
          current_snapshot: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num()} | nil,
          last_resend_time: non_neg_integer(),
          last_wal_write: {pid(), reference(), RaftEx.Domain.Types.index()},
          reader: term() | nil,
          mem_table: :ets.table() | nil,
          wal_name: atom(),
          meta_name: atom(),
          data_dir: Path.t(),
          tx: boolean(),
          pending: [term()],
          live_indexes: [RaftEx.Domain.Types.index()],
          last_term: RaftEx.Domain.Types.term_num(),
          last_written_index_term: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num()},
          entries: %{
            RaftEx.Domain.Types.index() =>
              {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}
          },
          next_index: RaftEx.Domain.Types.index()
        }

  # ---------------------------------------------------------------------------
  # Initialization
  # ---------------------------------------------------------------------------

  @doc """
  Initialize a new log instance.

  Sets up the data directory, WAL, ETS table for mem table, and recovers
  from existing state if present.
  """
  @spec init(map()) :: t()
  def init(%{uid: uid, system_config: %{data_dir: data_dir, names: names} = sys_cfg} = conf) do
    dir = server_data_dir(data_dir, uid)
    Filesystem.make_dir(dir)

    # Get process names
    wal_name = Map.fetch!(names, :wal)
    meta_name = Map.fetch!(names, :log_meta)
    _open_mem_tbls = Map.fetch!(names, :open_mem_tbls)

    # Segment-backed storage is opt-in (13.6); the in-memory log stays the
    # default until the read/recovery/compaction tests pass with it on.
    segment_enabled? =
      Map.get(conf, :segment_storage, Map.get(sys_cfg, :segment_storage, false)) == true

    segment_writer =
      if segment_enabled? do
        case Map.fetch(names, :segment_writer) do
          {:ok, name} -> name
          :error -> nil
        end
      else
        nil
      end

    log_window =
      Map.get(conf, :log_window_size, Map.get(sys_cfg, :log_window_size, 8192))

    # Create ETS table for this log's mem table
    mem_table = :ets.new(:"#{uid}_mem_table", [:set, :public, {:read_concurrency, true}])

    # Recover metadata
    _current_term = LogMeta.fetch(meta_name, uid, :current_term, 0)
    _voted_for = LogMeta.fetch(meta_name, uid, :voted_for)
    _last_applied = LogMeta.fetch(meta_name, uid, :last_applied, 0)

    # Recover from snapshot if exists
    {snapshot_index, snapshot_term, snapshot_state, snapshot_cluster} =
      recover_snapshot_state(dir)

    # Recover WAL entries
    {_wal_first, wal_last, wal_entries} = LogWal.recover(%{data_dir: dir})

    # Build entries map from WAL
    entries =
      wal_entries
      |> Enum.into(%{}, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)

    # Determine range
    range = compute_range(snapshot_index, entries, wal_last)
    next_index = compute_next_index(snapshot_index, entries, wal_last)
    last_term = compute_last_term(snapshot_term, entries, wal_last)

    %__MODULE__{
      cfg: conf,
      range: range,
      snapshot_state: snapshot_state,
      snapshot_cluster: snapshot_cluster,
      current_snapshot: if(snapshot_index > 0, do: {snapshot_index, snapshot_term}, else: nil),
      last_resend_time: 0,
      last_wal_write: {nil, make_ref(), -1},
      reader: nil,
      mem_table: mem_table,
      wal_name: wal_name,
      meta_name: meta_name,
      data_dir: dir,
      tx: false,
      pending: [],
      live_indexes: [],
      last_term: last_term,
      last_written_index_term: {wal_last, last_term},
      entries: entries,
      next_index: next_index,
      segment_writer: segment_writer,
      segment_enabled?: segment_enabled?,
      log_window: log_window
    }
  end

  defp compute_range(snapshot_index, entries, wal_last) do
    cond do
      snapshot_index > 0 and map_size(entries) > 0 ->
        {snapshot_index + 1, wal_last}

      snapshot_index > 0 ->
        {snapshot_index, snapshot_index}

      map_size(entries) > 0 ->
        {1, wal_last}

      true ->
        nil
    end
  end

  defp compute_next_index(snapshot_index, entries, wal_last) do
    cond do
      snapshot_index > 0 and map_size(entries) > 0 -> wal_last + 1
      snapshot_index > 0 -> snapshot_index + 1
      map_size(entries) > 0 -> wal_last + 1
      true -> 1
    end
  end

  # The last WAL record holds the highest index, so its term is the log's last
  # term. Looking it up is O(1); scanning the map for the maximum index was
  # O(n) on every startup.
  defp compute_last_term(snapshot_term, entries, wal_last) do
    case Map.get(entries, wal_last) do
      {_, term, _} -> term
      nil when snapshot_term > 0 -> snapshot_term
      nil -> 0
    end
  end

  @doc """
  Close the log and clean up resources.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{mem_table: mem_table}) do
    if mem_table, do: :ets.delete(mem_table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Append Operations
  # ---------------------------------------------------------------------------

  @doc """
  Append a single entry to the log.

  Returns the updated state. The entry is added to the mem table and
  scheduled for WAL write.
  """
  @spec append({RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}, t()) :: t()
  def append({idx, term, cmd} = entry, %__MODULE__{} = state) do
    # Add to entries map
    new_entries = Map.put(state.entries, idx, entry)

    # Add to ETS mem table
    if state.mem_table do
      :ets.insert(state.mem_table, {idx, term, cmd})
    end

    # Update state
    %{
      state
      | entries: new_entries,
        last_term: term,
        last_written_index_term: {idx, term},
        next_index: idx + 1,
        range: update_range(state.range, idx)
    }
  end

  @doc """
  Write multiple entries to the log.

  Appends all entries and then flushes to WAL.
  Returns `{:ok, updated_state}`.
  """
  @spec write([{RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}], t()) ::
          {:ok, t()} | {:error, term()}
  def write(entries, %__MODULE__{} = state) do
    case entries do
      [] -> {:ok, state}
      _ -> write_entries(entries, state)
    end
  end

  defp write_entries(entries, state) do
    # Write to WAL first (true write-ahead semantics).  Each server has its
    # own WAL file, selected by its uid.  Fixtures may build a log without
    # a WAL process; in that case the append is in-memory only.
    log_uid = if is_map(state.cfg), do: Map.get(state.cfg, :uid), else: nil

    case do_wal_write(state.wal_name, log_uid, entries) do
      {:ok, _, _} ->
        # WAL write succeeded, now update in-memory state in one pass.
        {:ok, apply_entries(entries, state)}

      {:error, reason} ->
        Logger.error("RaftEx.Infra.Persistence.Log: WAL write failed: #{inspect(reason)}")
        # Return error without modifying in-memory state
        {:error, reason}
    end
  end

  # Apply an already-durable batch. One ETS insert and one map fold per batch
  # keeps the mem table and entries map in step without per-entry overhead.
  defp apply_entries(entries, state) do
    if state.mem_table do
      :ets.insert(state.mem_table, entries)
    end

    new_entries =
      Enum.reduce(entries, state.entries, fn {idx, _, _} = entry, acc ->
        Map.put(acc, idx, entry)
      end)

    {first_idx, _, _} = hd(entries)
    {last_idx, last_term, _} = List.last(entries)

    range =
      case state.range do
        nil -> {first_idx, last_idx}
        {start, _} -> {start, last_idx}
      end

    %{
      state
      | entries: new_entries,
        last_term: last_term,
        last_written_index_term: {last_idx, last_term},
        next_index: last_idx + 1,
        range: range
    }
  end

  defp do_wal_write(nil, _uid, entries) do
    last = List.last(entries)
    {:ok, elem(last, 0), elem(last, 0)}
  end

  defp do_wal_write(wal_name, uid, entries), do: LogWal.append(wal_name, uid, entries)

  # ---------------------------------------------------------------------------
  # Read Operations
  # ---------------------------------------------------------------------------

  @doc """
  Fold over entries in the range `[from, to]`.

  Applies `fun` to each entry, accumulating `acc`. Returns `{final_acc, state}`.
  """
  @spec fold(
          RaftEx.Domain.Types.index(),
          RaftEx.Domain.Types.index(),
          (term(), term() -> term()),
          term(),
          t(),
          atom()
        ) :: {term(), t()}
  @spec fold(
          RaftEx.Domain.Types.index(),
          RaftEx.Domain.Types.index(),
          (term(), term() -> term()),
          term(),
          t(),
          atom()
        ) :: {term(), t()}

  def fold(from, to, fun, acc, state, strategy \\ :error)

  def fold(from, to, fun, acc, %__MODULE__{range: {start, _ending}} = state, :error)
      when to >= from and to >= start do
    found = entries_in_range(state.entries, from, to)
    # Cold entries evicted to sealed segments are merged back so a range
    # spanning memory and segments reads correctly (13.2).
    all =
      (found ++ backfill_from_segments(state, from, to, found))
      |> Enum.sort_by(fn {idx, _, _} -> idx end)
      |> Enum.uniq_by(fn {idx, _, _} -> idx end)

    {final_acc, _} =
      Enum.reduce(all, {acc, state}, fn entry, {acc_acc, st} ->
        {fun.(entry, acc_acc), st}
      end)

    {final_acc, state}
  end

  def fold(from, to, fun, acc, %__MODULE__{} = state, :return) do
    fold(from, to, fun, acc, state, :error)
  end

  def fold(_from, _to, _fun, acc, state, _strategy), do: {acc, state}

  # Fetch indexes missing from memory from sealed segments. Returns [] when
  # segment storage is disabled, unavailable, or fails.
  defp backfill_from_segments(
         %{segment_enabled?: true, segment_writer: seg} = _state,
         from,
         to,
         found
       )
       when not is_nil(seg) do
    have = MapSet.new(found, fn {idx, _, _} -> idx end)
    missing = for idx <- from..to//1, not MapSet.member?(have, idx), do: idx

    case missing do
      [] ->
        []

      [first | _] ->
        last = List.last(missing)

        try do
          case LogSegmentWriter.read(seg, first, last) do
            {:ok, entries} -> entries
            _ -> []
          end
        rescue
          _ -> []
        catch
          _, _ -> []
        end
    end
  end

  defp backfill_from_segments(_state, _from, _to, _found), do: []

  # A short range over a large log is probed index by index, which avoids
  # scanning the whole entries map. A range wider than the map (a sparse log,
  # or a range larger than the log) falls back to filtering the map.
  defp entries_in_range(entries, from, to) do
    span = to - from + 1

    if span <= map_size(entries) do
      for idx <- from..to, entry = Map.get(entries, idx), do: entry
    else
      entries
      |> Enum.filter(fn {idx, _} -> idx >= from and idx <= to end)
      |> Enum.sort_by(fn {idx, _} -> idx end)
      |> Enum.map(fn {_idx, entry} -> entry end)
    end
  end

  @doc """
  Read specific entries by index.

  Returns `{entries, state}` where entries is a list of `{index, term, command}` tuples.
  """
  @spec sparse_read([RaftEx.Domain.Types.index()], t()) :: {[term()], t()}
  def sparse_read(indexes, %__MODULE__{} = state) do
    {found, missing} =
      Enum.reduce(indexes, {[], []}, fn idx, {es, ms} ->
        case Map.get(state.entries, idx) do
          nil -> {es, [idx | ms]}
          entry -> {[entry | es], ms}
        end
      end)

    extra =
      case {missing, state} do
        {[], _} ->
          []

        {_, %{segment_enabled?: true, segment_writer: seg}} when not is_nil(seg) ->
          wanted = MapSet.new(missing)

          try do
            case LogSegmentWriter.read(seg, Enum.min(missing), Enum.max(missing)) do
              {:ok, entries} ->
                Enum.filter(entries, fn {idx, _, _} -> MapSet.member?(wanted, idx) end)

              _ ->
                []
            end
          rescue
            _ -> []
          catch
            _, _ -> []
          end

        _ ->
          []
      end

    {Enum.sort_by(Enum.reverse(found) ++ extra, fn {idx, _, _} -> idx end), state}
  end

  @doc """
  Build a read plan for partial reads.

  Returns a map with `:dir`, `:read`, and `:plan` keys indicating
  which entries are in memory vs need to be read from disk.
  """
  @spec partial_read(
          [RaftEx.Domain.Types.index()],
          t(),
          (RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term() -> term())
        ) :: %{dir: Path.t(), read: map(), plan: list()}
  def partial_read(indexes, %__MODULE__{} = state, _transform_fun) do
    {in_memory, need_disk} =
      Enum.split_with(indexes, fn idx ->
        Map.has_key?(state.entries, idx)
      end)

    read_map =
      in_memory
      |> Enum.into(%{}, fn idx ->
        {idx, Map.get(state.entries, idx)}
      end)

    plan =
      Enum.map(need_disk, fn idx ->
        {:read_from_segment, idx}
      end)

    %{
      dir: state.data_dir,
      read: read_map,
      plan: plan
    }
  end

  @doc """
  Execute a read plan, fetching entries from disk as needed.
  """
  @spec execute_read_plan(map(), term(), (term() -> term()), keyword()) ::
          {map(), term()}
  def execute_read_plan(plan, flru, transform_fun, _options) do
    # In a full implementation, this would read from segment files
    # For now, return what we have in the plan
    results =
      plan.read
      |> Map.new(fn {idx, entry} ->
        case entry do
          {^idx, term, cmd} ->
            {idx, transform_fun.(idx, term, cmd)}

          _ ->
            {idx, nil}
        end
      end)

    {results, flru || LruCache.new(1, nil)}
  end

  @doc """
  Get information about a read plan.
  """
  @spec read_plan_info(map()) :: %{
          num_read: non_neg_integer(),
          num_in_segments: non_neg_integer(),
          num_segments: non_neg_integer()
        }
  def read_plan_info(plan) do
    %{
      num_read: map_size(plan.read),
      num_in_segments: length(plan.plan),
      num_segments: 0
    }
  end

  # ---------------------------------------------------------------------------
  # Index and Term Queries
  # ---------------------------------------------------------------------------

  @doc """
  Get the last index and term in the log.
  """
  @spec last_index_term(t()) :: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num()}
  def last_index_term(%__MODULE__{range: {_, last}, last_term: lt}), do: {last, lt}
  def last_index_term(%__MODULE__{current_snapshot: nil}), do: {0, 0}
  def last_index_term(%__MODULE__{current_snapshot: cs}), do: cs

  @doc """
  Get the last written index and term.
  """
  @spec last_written(t()) :: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num()}
  def last_written(%__MODULE__{last_written_index_term: lwit}), do: lwit

  @doc """
  Set the last index (used during recovery).
  """
  @spec set_last_index(RaftEx.Domain.Types.index(), t()) :: {:ok, t()}
  def set_last_index(idx, %__MODULE__{} = state) do
    {:ok, %{state | range: update_range(state.range, idx)}}
  end

  @doc """
  Get the next index that would be assigned.
  """
  @spec next_index(t()) :: RaftEx.Domain.Types.index()
  def next_index(%__MODULE__{next_index: next}) when next > 0, do: next
  def next_index(%__MODULE__{range: {_, last}}), do: last + 1
  def next_index(%__MODULE__{current_snapshot: {snap_idx, _}}), do: snap_idx + 1
  def next_index(%__MODULE__{current_snapshot: nil}), do: 0

  @doc """
  Fetch the term for a specific index.
  """
  @spec fetch_term(RaftEx.Domain.Types.index(), t()) ::
          {RaftEx.Domain.Types.term_num() | nil, t()}
  def fetch_term(idx, %__MODULE__{} = state) do
    case Map.get(state.entries, idx) do
      {^idx, term, _} -> {term, state}
      nil -> fetch_term_from_segment(idx, state)
    end
  end

  # Cold read for entries evicted to sealed segments (13.2). When segment
  # storage is disabled this is a plain miss.
  defp fetch_term_from_segment(idx, %{segment_enabled?: true, segment_writer: seg} = state)
       when not is_nil(seg) do
    try do
      case LogSegmentWriter.read_entry(seg, idx) do
        {:ok, {^idx, term, _}} -> {term, state}
        _ -> {nil, state}
      end
    rescue
      _ -> {nil, state}
    catch
      _, _ -> {nil, state}
    end
  end

  defp fetch_term_from_segment(_idx, state), do: {nil, state}

  # ---------------------------------------------------------------------------
  # Snapshot Operations
  # ---------------------------------------------------------------------------

  @doc """
  Get the current snapshot state.
  """
  @spec snapshot_state(t()) :: term() | nil
  def snapshot_state(%__MODULE__{snapshot_state: ss}), do: ss

  @doc """
  Get the size of the current snapshot.
  """
  @spec snapshot_size(t()) :: non_neg_integer() | nil
  def snapshot_size(%__MODULE__{} = state) do
    file = Path.join(Path.join(state.data_dir, "snapshot"), "snapshot.dat")

    case File.stat(file) do
      {:ok, stat} -> stat.size
      {:error, _} -> nil
    end
  end

  @doc """
  Set the snapshot state.
  """
  @spec set_snapshot_state(term(), t()) :: t()
  def set_snapshot_state(snap_state, state), do: %{state | snapshot_state: snap_state}

  @doc """
  Install a snapshot at the given index and term.

  This clears all log entries up to and including the snapshot index.
  """
  @spec install_snapshot(
          {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num()},
          module(),
          [RaftEx.Domain.Types.index()],
          t()
        ) :: {:ok, t(), [term()]}
  def install_snapshot({snap_idx, snap_term} = idx_term, _mac_mod, live_indexes, state) do
    uid = if is_map(state.cfg), do: Map.get(state.cfg, :uid), else: nil

    if uid != nil and state.wal_name != nil do
      try do
        LogWal.truncate(state.wal_name, uid, snap_idx)
      catch
        _, _ -> :ok
      end
    end

    # Remove entries up to snapshot index
    new_entries =
      state.entries
      |> Enum.reject(fn {idx, _} -> idx <= snap_idx end)
      |> Enum.into(%{})

    # Clear ETS table entries up to snapshot
    if state.mem_table do
      :ets.select_delete(state.mem_table, [{{:"$1", :_, :_}, [{:"=<", :"$1", snap_idx}], [true]}])
    end

    new_range =
      if map_size(new_entries) > 0 do
        {first_idx, _} = Enum.min_by(new_entries, fn {idx, _} -> idx end)
        {last_idx, _} = Enum.max_by(new_entries, fn {idx, _} -> idx end)
        {first_idx, last_idx}
      else
        {snap_idx, snap_idx}
      end

    # The installed snapshot is the new durability floor: term tracking
    # follows the retained entries, or the snapshot itself when none remain.
    {last_idx, last_term} =
      case Enum.max_by(new_entries, fn {i, _} -> i end, fn -> nil end) do
        {i, {_, t, _}} -> {i, t}
        nil -> {snap_idx, snap_term}
      end

    new_state = %{
      state
      | current_snapshot: idx_term,
        snapshot_state: nil,
        entries: new_entries,
        range: new_range,
        last_term: last_term,
        last_written_index_term: {last_idx, last_term},
        live_indexes: live_indexes,
        next_index: max(snap_idx, last_idx) + 1
    }

    {:ok, new_state, []}
  end

  @doc """
  Recover snapshot state from disk.
  """
  @spec recover_snapshot(t()) :: term() | nil
  def recover_snapshot(%__MODULE__{snapshot_state: ss}), do: ss

  @doc """
  Membership recorded in the loaded snapshot, or `nil` for legacy snapshots.
  """
  @spec snapshot_cluster(t()) :: map() | nil
  def snapshot_cluster(%__MODULE__{snapshot_cluster: cluster}), do: cluster

  @doc """
  Get the current snapshot index and term.
  """
  @spec snapshot_index_term(t()) ::
          {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num()} | nil
  def snapshot_index_term(%__MODULE__{current_snapshot: cs}), do: cs

  # ---------------------------------------------------------------------------
  # Checkpoint and Release Cursor
  # ---------------------------------------------------------------------------

  @doc """
  Update the release cursor for snapshot management.
  """
  @spec update_release_cursor(
          RaftEx.Domain.Types.index(),
          atom(),
          {module(), term()},
          term(),
          t()
        ) :: {t(), [term()]}
  def update_release_cursor(idx, cluster, mac_ctx, mac_state, state) do
    checkpoint(idx, cluster, mac_ctx, mac_state, state)
  end

  @doc """
  Create a checkpoint at the given index.
  """
  @spec checkpoint(
          RaftEx.Domain.Types.index(),
          atom(),
          map(),
          term(),
          t()
        ) :: {t(), [term()]}
  def checkpoint(_idx, _cluster, _mac_ctx, _mac_state, %__MODULE__{cfg: cfg} = state)
      when not is_map(cfg),
      do: {state, []}

  def checkpoint(_idx, _cluster, _mac_ctx, _mac_state, %__MODULE__{data_dir: dir} = state)
      when not is_binary(dir),
      do: {state, []}

  def checkpoint(idx, cluster, {mac_ver, _mac_mod}, mac_state, %__MODULE__{} = state) do
    snap_mod = Map.get(state.cfg, :snapshot_module, LogSnapshot)
    snap_dir = Path.join(state.data_dir, "snapshot")
    :ok = Filesystem.make_dir(snap_dir)

    {term, _} = fetch_term(idx, state)
    snap_term = if is_integer(term), do: term, else: Map.get(state, :last_term, 0)

    meta = %{
      index: idx,
      term: snap_term,
      cluster: storable_cluster(cluster),
      machine_version: mac_ver
    }

    case snap_mod.write(snap_dir, meta, mac_state, true) do
      {:ok, _bytes} ->
        compact_after_snapshot(idx, snap_term, mac_state, state)

      {:error, reason} ->
        Logger.error("RaftEx.Log: checkpoint failed: #{inspect(reason)}")
        {state, []}
    end
  end

  defp compact_after_snapshot(idx, snap_term, mac_state, state) do
    # When segment storage is enabled, seal the compacted range first so
    # cold reads keep working after the in-memory window is evicted; then
    # drop fully covered segments (retention).
    state =
      case state do
        %{segment_enabled?: true} ->
          case seal_oldest(state, idx) do
            {:ok, sealed} ->
              delete_segments_up_to(sealed, idx)
              sealed

            {:error, _} ->
              state
          end

        _ ->
          state
      end

    new_entries =
      state.entries
      |> Enum.reject(fn {i, _} -> i <= idx end)
      |> Enum.into(%{})

    if state.mem_table do
      :ets.select_delete(state.mem_table, [{{:"$1", :_, :_}, [{:"=<", :"$1", idx}], [true]}])
    end

    new_range = range_from_entries_or(new_entries, {idx, idx})

    {last_idx, last_term} =
      case Enum.max_by(new_entries, fn {i, _} -> i end, fn -> nil end) do
        {i, {_, t, _}} -> {i, t}
        nil -> {idx, snap_term}
      end

    uid = Map.get(state.cfg, :uid)

    if uid != nil and state.wal_name != nil do
      safe_truncate(state.wal_name, uid, idx)
    end

    new_state = %{
      state
      | current_snapshot: {idx, snap_term},
        snapshot_state: mac_state,
        entries: new_entries,
        range: new_range,
        last_term: last_term,
        last_written_index_term: {last_idx, last_term},
        next_index: max(idx, last_idx) + 1
    }

    {new_state, []}
  end

  defp range_from_entries_or(entries, fallback) do
    if map_size(entries) == 0 do
      fallback
    else
      keys = Map.keys(entries)
      {Enum.min(keys), Enum.max(keys)}
    end
  end

  defp safe_truncate(wal_name, uid, index) do
    LogWal.truncate(wal_name, uid, index)
  catch
    _, _ -> :ok
  end

  @doc """
  Promote a checkpoint to be the new recovery point.
  """
  @spec promote_checkpoint(RaftEx.Domain.Types.index(), t()) :: {t(), [term()]}
  def promote_checkpoint(idx, state) do
    # Update metadata
    LogMeta.store_sync(state.meta_name, state.cfg.uid, :last_applied, idx)

    {state, []}
  end

  # ---------------------------------------------------------------------------
  # Tick and Maintenance
  # ---------------------------------------------------------------------------

  @doc """
  Process a tick for log maintenance.
  """
  @spec tick(non_neg_integer(), t()) :: t()
  def tick(now, state), do: %{state | last_resend_time: now}

  @doc """
  Truncate the log, removing all entries starting from (and including) `from_index`.
  Used when a leader sends conflicting entries at a given index.
  """
  @spec truncate(RaftEx.Domain.Types.index(), t()) :: t()
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def truncate(from_index, %__MODULE__{entries: entries, range: range} = state) do
    truncate_wal_from(state, from_index)

    {new_entries, new_last} = truncate_entries(entries, range, from_index)

    new_range =
      case new_last do
        nil -> nil
        {idx, _, _} -> normalize_range(range, idx)
      end

    {new_last_index, new_last_term} = last_index_and_term(new_last)

    if state.mem_table do
      :ets.select_delete(state.mem_table, [
        {{:"$1", :_, :_}, [{:>=, :"$1", from_index}], [true]}
      ])
    end

    %{
      state
      | entries: new_entries,
        range: new_range,
        last_term: new_last_term,
        last_written_index_term: {new_last_index, new_last_term},
        next_index: if(new_last_index > 0, do: new_last_index + 1, else: from_index)
    }
  end

  defp truncate_entries(entries, range, from_index) do
    case range do
      {_start, last} when last >= from_index ->
        {Map.drop(entries, from_index..last), probe_entry(entries, from_index - 1)}

      {_start, last} ->
        {entries, Map.get(entries, last)}

      nil ->
        {entries, nil}
    end
  end

  defp last_index_and_term(nil), do: {0, 0}
  defp last_index_and_term({idx, term, _}), do: {idx, term}

  # Entries are dense in normal operation, so the new last index is usually
  # `from_index - 1`. Probe downward to tolerate sparse logs without scanning
  # the whole map.
  defp probe_entry(_entries, index) when index < 1, do: nil

  defp probe_entry(entries, index) do
    case Map.get(entries, index) do
      nil -> probe_entry(entries, index - 1)
      entry -> entry
    end
  end

  defp normalize_range({start, _last}, idx) when start <= idx, do: {start, idx}
  defp normalize_range(_range, idx), do: {idx, idx}

  defp truncate_wal_from(state, from_index) do
    uid = if is_map(state.cfg), do: Map.get(state.cfg, :uid), else: nil
    wal_name = state.wal_name

    if uid != nil and wal_name != nil do
      safe_truncate_from(wal_name, uid, from_index)
    end
  end

  defp safe_truncate_from(wal_name, uid, from_index) do
    LogWal.truncate_from(wal_name, uid, from_index)
  catch
    _, _ -> :ok
  end

  @doc """
  Atomically replace the log suffix starting at `from_index` with
  `new_entries`, both in memory and in the WAL.

  The WAL rewrite is temp-write/rename atomic, so a crash leaves either the
  complete old suffix or the complete new suffix. Returns `{:ok, new_state}`
  or `{:error, reason}` with the state unchanged.
  """
  @spec replace_from(RaftEx.Domain.Types.index(), list(), t()) ::
          {:ok, t()} | {:error, term()}
  def replace_from(from_index, new_entries, %__MODULE__{} = state) do
    uid = if is_map(state.cfg), do: Map.get(state.cfg, :uid), else: nil
    wal_name = state.wal_name

    wal_result =
      if wal_name != nil do
        try do
          LogWal.replace_from(wal_name, uid, from_index, new_entries)
        catch
          _, reason -> {:error, reason}
        end
      else
        :ok
      end

    case wal_result do
      :ok ->
        truncated = truncate_state(state, from_index)

        case new_entries do
          [] -> {:ok, truncated}
          _ -> {:ok, apply_entries(new_entries, truncated)}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp truncate_state(%__MODULE__{entries: entries, range: range} = state, from_index) do
    {new_entries, new_last} = truncate_entries(entries, range, from_index)

    new_range =
      case new_last do
        nil -> nil
        {idx, _, _} -> normalize_range(range, idx)
      end

    {new_last_index, new_last_term} = last_index_and_term(new_last)

    if state.mem_table do
      :ets.select_delete(state.mem_table, [
        {{:"$1", :_, :_}, [{:>=, :"$1", from_index}], [true]}
      ])
    end

    %{
      state
      | entries: new_entries,
        range: new_range,
        last_term: new_last_term,
        last_written_index_term: {new_last_index, new_last_term},
        next_index: if(new_last_index > 0, do: new_last_index + 1, else: from_index)
    }
  end

  @doc """
  Check if the log can accept writes.
  """
  @spec can_write?(t()) :: boolean()
  def can_write?(%__MODULE__{}), do: true

  @doc """
  Check if an entry exists in the log.
  """
  @spec exists({RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num()}, t()) ::
          {boolean(), t()}
  def exists({idx, term}, state) do
    case fetch_term(idx, state) do
      {^term, s} -> {true, s}
      {_, s} -> {false, s}
    end
  end

  @doc """
  Check if there are pending writes.
  """
  @spec has_pending?(t()) :: boolean()
  def has_pending?(%__MODULE__{pending: []}), do: false
  def has_pending?(%__MODULE__{}), do: true

  @doc """
  Get an overview of the log state.
  """
  @spec overview(t()) :: map()
  def overview(%__MODULE__{} = state) do
    %{
      type: RaftEx.Infra.Persistence.Log,
      range: state.range,
      current_snapshot: state.current_snapshot,
      last_term: state.last_term,
      next_index: state.next_index,
      entries_count: map_size(state.entries),
      pending_count: length(state.pending)
    }
  end

  # ---------------------------------------------------------------------------
  # Configuration Persistence
  # ---------------------------------------------------------------------------

  @doc """
  Write the server configuration to disk atomically (temp write, fsync,
  rename, directory sync) so a crash can never leave a torn config file.
  """
  @spec write_config(map(), t()) :: :ok | {:error, term()}
  def write_config(config, %__MODULE__{cfg: %{uid: uid} = cfg}) do
    dir = server_data_dir(cfg.system_config.data_dir, uid)
    write_config_to_dir(dir, config)
  end

  @doc """
  Atomically write a server configuration map into `dir/config`.
  """
  @spec write_config_to_dir(Path.t(), map()) :: :ok | {:error, term()}
  def write_config_to_dir(dir, config) when is_binary(dir) and is_map(config) do
    config_path = Path.join(dir, "config")
    tmp_path = config_path <> ".tmp"
    clean = Map.drop(config, [:parent, :counter, :has_changed, :system_config])
    payload = :erlang.term_to_binary(clean)

    with :ok <- File.mkdir_p(dir),
         :ok <- Filesystem.write_file(tmp_path, payload, true),
         :ok <- Filesystem.rename(tmp_path, config_path),
         :ok <- sync_dir(config_path) do
      :ok
    else
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end

  defp sync_dir(file_path) do
    dir = Path.dirname(file_path)

    try do
      case :file.open(dir, [:read, :binary, :raw]) do
        {:ok, dir_fd} ->
          result = :file.sync(dir_fd)
          :file.close(dir_fd)
          result

        {:error, :eisdir} ->
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    catch
      _, reason -> {:error, reason}
    end
  end

  @doc """
  Read the cluster configuration from disk.
  """
  @spec read_config(t() | Path.t()) :: {:ok, map()} | {:error, term()}
  def read_config(%__MODULE__{cfg: %{uid: uid, system_config: %{data_dir: dd}}}),
    do: read_config(server_data_dir(dd, uid))

  def read_config(dir) do
    path = Path.join(dir, "config")

    case File.read(path) do
      {:ok, contents} ->
        # Try binary term first, then fall back to eval_string for backwards compatibility
        try do
          term = :erlang.binary_to_term(contents)
          {:ok, term}
        rescue
          _ ->
            try do
              {term, _} = Code.eval_string(contents)
              {:ok, term}
            rescue
              err -> {:error, err}
            end
        end

      err ->
        err
    end
  end

  @doc """
  Delete all log data (used for cleanup).
  """
  @spec delete_everything(t()) :: :ok
  def delete_everything(%__MODULE__{cfg: %{uid: uid}} = state) do
    close(state)
    dir = server_data_dir(state.data_dir, uid)
    Filesystem.recursive_delete(dir)
    :ok
  end

  @doc """
  Seal the oldest log range up to `through_index` into segments and evict
  it from the in-memory window.

  The entries are written to sealed segments and synced first; only then
  are they evicted from memory. A crash between the write and the eviction
  leaves the data in both places, which recovery deduplicates by index —
  so sealing is atomic in effect (13.3). Returns `{:ok, log}` or
  `{:error, reason}` (a gap in the retained range aborts without evicting).
  """
  @spec seal_oldest(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  def seal_oldest(%__MODULE__{segment_writer: nil} = _log, _through_index) do
    {:error, :no_segment_writer}
  end

  def seal_oldest(%__MODULE__{entries: entries} = log, through_index) do
    oldest =
      case Map.keys(entries) do
        [] -> nil
        keys -> Enum.min(keys)
      end

    cond do
      is_nil(oldest) or through_index < oldest ->
        {:ok, log}

      true ->
        idxs = Enum.to_list(oldest..through_index//1)

        if Enum.all?(idxs, &Map.has_key?(entries, &1)) do
          to_seal = Enum.map(idxs, &Map.fetch!(entries, &1))

          with :ok <- LogSegmentWriter.write(log.segment_writer, to_seal) do
            {:ok, evict_range(log, oldest, through_index)}
          end
        else
          {:error, {:gap_in_range, oldest, through_index}}
        end
    end
  end

  # Evict a sealed prefix from the in-memory window and mem table.
  defp evict_range(%__MODULE__{entries: entries, mem_table: mem_table} = log, from, through) do
    kept = Map.drop(entries, Enum.to_list(from..through//1))

    if mem_table do
      try do
        Enum.each(from..through//1, &:ets.delete(mem_table, &1))
      rescue
        _ -> :ok
      end
    end

    range =
      case Map.keys(kept) do
        [] -> log.range
        keys -> {Enum.min(keys), Enum.max(keys)}
      end

    %{log | entries: kept, range: range}
  end

  @doc """
  Delete segments at or below `up_to_index`, retaining partially covered
  segments (13.5). No-op when segment storage is disabled.
  """
  @spec delete_segments_up_to(t(), non_neg_integer()) :: :ok | {:error, term()}
  def delete_segments_up_to(%{segment_writer: nil}, _up_to_index), do: :ok

  def delete_segments_up_to(%{segment_writer: seg}, up_to_index) do
    try do
      LogSegmentWriter.delete_up_to(seg, up_to_index)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  @doc """
  Release resources (file handles, etc.).
  """
  @spec release_resources(non_neg_integer(), atom(), t()) :: t()
  def release_resources(_max_open, _access_pattern, state), do: state

  @doc """
  Handle a log event (e.g., compaction notification).
  """
  @spec handle_event(term(), t()) :: {t(), [term()]}
  def handle_event(_evt, %__MODULE__{} = state), do: {state, []}

  # ---------------------------------------------------------------------------
  # Private Helpers
  # ---------------------------------------------------------------------------

  defp server_data_dir(dir, uid), do: Path.join(dir, uid)

  defp update_range(nil, idx), do: {idx, idx}
  defp update_range({start, _end}, idx), do: {start, idx}

  defp recover_snapshot_state(dir) do
    snapshot_dir = Path.join(dir, "snapshot")

    case :prim_file.read_file_info(snapshot_dir) do
      {:ok, _} ->
        case LogSnapshot.recover(snapshot_dir) do
          {:ok, meta, mac_state} when is_map(meta) ->
            snap_idx = Map.get(meta, :index, 0)
            snap_term = Map.get(meta, :term, 0)

            if is_integer(snap_idx) and is_integer(snap_term) and snap_idx >= 0 do
              {snap_idx, snap_term, mac_state, snapshot_meta_cluster(meta)}
            else
              Logger.warning(
                "RaftEx.Log: ignoring snapshot with invalid metadata, falling back to WAL"
              )

              {0, 0, nil, nil}
            end

          {:error, reason} ->
            Logger.warning(
              "RaftEx.Log: snapshot unreadable (#{inspect(reason)}), falling back to WAL"
            )

            {0, 0, nil, nil}

          _ ->
            {0, 0, nil, nil}
        end

      _ ->
        {0, 0, nil, nil}
    end
  rescue
    e ->
      Logger.warning("RaftEx.Log: snapshot recovery raised #{inspect(e)}, falling back to WAL")
      {0, 0, nil, nil}
  catch
    _, reason ->
      Logger.warning(
        "RaftEx.Log: snapshot recovery threw #{inspect(reason)}, falling back to WAL"
      )

      {0, 0, nil, nil}
  end

  defp snapshot_meta_cluster(meta) do
    case Map.get(meta, :cluster) do
      cluster when is_map(cluster) and map_size(cluster) > 0 -> cluster
      _ -> nil
    end
  end

  # Membership roles recorded in a snapshot, with voter uids preserved.
  # Anything else (legacy snapshots, malformed input) records no cluster.
  defp storable_cluster(cluster) when is_map(cluster) do
    Map.new(cluster, fn {id, peer} ->
      {id, %{voter_status: peer_voter_status(peer)}}
    end)
  end

  defp storable_cluster(_), do: %{}

  defp peer_voter_status(%{voter_status: %{membership: m, uid: uid}})
       when m in [:voter, :promotable, :non_voter] do
    %{membership: m, uid: uid}
  end

  defp peer_voter_status(%{voter_status: %{membership: m}})
       when m in [:voter, :promotable, :non_voter] do
    %{membership: m, uid: nil}
  end

  defp peer_voter_status(_), do: %{membership: :voter, uid: nil}
end
