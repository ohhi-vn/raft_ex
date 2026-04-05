defmodule RaftEx.Log do
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

  alias RaftEx.LogMeta
  alias RaftEx.LogWal

  # ---------------------------------------------------------------------------
  # State
  # ---------------------------------------------------------------------------

  defstruct [
    :cfg,
    :range,
    :snapshot_state,
    :current_snapshot,
    :last_resend_time,
    :last_wal_write,
    :reader,
    :mem_table,
    :wal_name,
    :meta_name,
    :data_dir,
    tx: false,
    pending: [],
    live_indexes: [],
    last_term: 0,
    last_written_index_term: {0, 0},
    entries: %{},
    next_index: 0
  ]

  @type t :: %__MODULE__{
          cfg: map(),
          range: {RaftEx.Types.index(), RaftEx.Types.index()} | nil,
          snapshot_state: term() | nil,
          current_snapshot: {RaftEx.Types.index(), RaftEx.Types.term_num()} | nil,
          last_resend_time: non_neg_integer(),
          last_wal_write: {pid(), reference(), RaftEx.Types.index()},
          reader: term() | nil,
          mem_table: :ets.table() | nil,
          wal_name: atom(),
          meta_name: atom(),
          data_dir: Path.t(),
          tx: boolean(),
          pending: [term()],
          live_indexes: [RaftEx.Types.index()],
          last_term: RaftEx.Types.term_num(),
          last_written_index_term: {RaftEx.Types.index(), RaftEx.Types.term_num()},
          entries: %{
            RaftEx.Types.index() => {RaftEx.Types.index(), RaftEx.Types.term_num(), term()}
          },
          next_index: RaftEx.Types.index()
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
  def init(%{uid: uid, system_config: %{data_dir: data_dir, names: names}} = conf) do
    dir = server_data_dir(data_dir, uid)
    RaftEx.Lib.make_dir(dir)

    # Get process names
    wal_name = Map.fetch!(names, :wal)
    meta_name = Map.fetch!(names, :log_meta)
    open_mem_tbls = Map.fetch!(names, :open_mem_tbls)

    # Create ETS table for this log's mem table
    mem_table = :ets.new(:"#{uid}_mem_table", [:set, :public, {:read_concurrency, true}])

    # Recover metadata
    current_term = LogMeta.fetch(meta_name, uid, :current_term, 0)
    voted_for = LogMeta.fetch(meta_name, uid, :voted_for)
    last_applied = LogMeta.fetch(meta_name, uid, :last_applied, 0)

    # Recover from snapshot if exists
    {snapshot_index, snapshot_term, snapshot_state} = recover_snapshot_state(dir)

    # Recover WAL entries
    {wal_first, wal_last, wal_entries} = LogWal.recover(%{data_dir: dir})

    # Build entries map from WAL
    entries =
      wal_entries
      |> Enum.into(%{}, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)

    # Determine range
    range =
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

    next_index =
      cond do
        snapshot_index > 0 and map_size(entries) > 0 -> wal_last + 1
        snapshot_index > 0 -> snapshot_index + 1
        map_size(entries) > 0 -> wal_last + 1
        true -> 1
      end

    last_term =
      cond do
        map_size(entries) > 0 ->
          {_, {_, term, _}} = Enum.max_by(entries, fn {idx, _} -> idx end)
          term

        snapshot_term > 0 ->
          snapshot_term

        true ->
          0
      end

    %__MODULE__{
      cfg: conf,
      range: range,
      snapshot_state: snapshot_state,
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
      next_index: next_index
    }
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
  @spec append({RaftEx.Types.index(), RaftEx.Types.term_num(), term()}, t()) :: t()
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
  @spec write([{RaftEx.Types.index(), RaftEx.Types.term_num(), term()}], t()) ::
          {:ok, t()} | {:error, term()}
  def write(entries, %__MODULE__{} = state) do
    case entries do
      [] ->
        {:ok, state}

      _ ->
        # Write to WAL first (true write-ahead semantics)
        case LogWal.append(state.wal_name, entries) do
          {:ok, _, _} ->
            # WAL write succeeded, now update in-memory state
            new_state =
              Enum.reduce(entries, state, fn entry, acc ->
                append(entry, acc)
              end)

            {:ok, new_state}

          {:error, reason} ->
            Logger.error("RaftEx.Log: WAL write failed: #{inspect(reason)}")
            # Return error without modifying in-memory state
            {:error, reason}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Read Operations
  # ---------------------------------------------------------------------------

  @doc """
  Fold over entries in the range `[from, to]`.

  Applies `fun` to each entry, accumulating `acc`. Returns `{final_acc, state}`.
  """
  @spec fold(
          RaftEx.Types.index(),
          RaftEx.Types.index(),
          (term(), term() -> term()),
          term(),
          t(),
          atom()
        ) :: {term(), t()}
  def fold(from, to, fun, acc, %__MODULE__{range: {start, ending}} = state, strategy \\ :error)
      when to >= from and to >= start do
    entries_to_read =
      state.entries
      |> Enum.filter(fn {idx, _} -> idx >= from and idx <= to end)
      |> Enum.sort_by(fn {idx, _} -> idx end)

    {final_acc, _} =
      Enum.reduce(entries_to_read, {acc, state}, fn {_idx, entry}, {acc_acc, st} ->
        {fun.(entry, acc_acc), st}
      end)

    {final_acc, state}
  end

  def fold(_from, _to, _fun, acc, state, _), do: {acc, state}

  @doc """
  Read specific entries by index.

  Returns `{entries, state}` where entries is a list of `{index, term, command}` tuples.
  """
  @spec sparse_read([RaftEx.Types.index()], t()) :: {[term()], t()}
  def sparse_read(indexes, %__MODULE__{} = state) do
    entries =
      Enum.flat_map(indexes, fn idx ->
        case Map.get(state.entries, idx) do
          nil -> []
          entry -> [entry]
        end
      end)

    {entries, state}
  end

  @doc """
  Build a read plan for partial reads.

  Returns a map with `:dir`, `:read`, and `:plan` keys indicating
  which entries are in memory vs need to be read from disk.
  """
  @spec partial_read(
          [RaftEx.Types.index()],
          t(),
          (RaftEx.Types.index(), RaftEx.Types.term_num(), term() -> term())
        ) :: %{dir: Path.t(), read: map(), plan: list()}
  def partial_read(indexes, %__MODULE__{} = state, transform_fun) do
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
  def execute_read_plan(plan, flru, transform_fun, options) do
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

    {results, flru || RaftEx.LruCache.new(1, nil)}
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
  @spec last_index_term(t()) :: {RaftEx.Types.index(), RaftEx.Types.term_num()}
  def last_index_term(%__MODULE__{range: {_, last}, last_term: lt}), do: {last, lt}
  def last_index_term(%__MODULE__{current_snapshot: nil}), do: {0, 0}
  def last_index_term(%__MODULE__{current_snapshot: cs}), do: cs

  @doc """
  Get the last written index and term.
  """
  @spec last_written(t()) :: {RaftEx.Types.index(), RaftEx.Types.term_num()}
  def last_written(%__MODULE__{last_written_index_term: lwit}), do: lwit

  @doc """
  Set the last index (used during recovery).
  """
  @spec set_last_index(RaftEx.Types.index(), t()) :: {:ok, t()}
  def set_last_index(idx, %__MODULE__{} = state) do
    {:ok, %{state | range: update_range(state.range, idx)}}
  end

  @doc """
  Get the next index that would be assigned.
  """
  @spec next_index(t()) :: RaftEx.Types.index()
  def next_index(%__MODULE__{next_index: next}) when next > 0, do: next
  def next_index(%__MODULE__{range: {_, last}}), do: last + 1
  def next_index(%__MODULE__{current_snapshot: {snap_idx, _}}), do: snap_idx + 1
  def next_index(%__MODULE__{current_snapshot: nil}), do: 0

  @doc """
  Fetch the term for a specific index.
  """
  @spec fetch_term(RaftEx.Types.index(), t()) :: {RaftEx.Types.term_num() | nil, t()}
  def fetch_term(idx, %__MODULE__{} = state) do
    case Map.get(state.entries, idx) do
      {^idx, term, _} -> {term, state}
      nil -> {nil, state}
    end
  end

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
  def snapshot_size(%__MODULE__{} = _state), do: nil

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
          {RaftEx.Types.index(), RaftEx.Types.term_num()},
          module(),
          [RaftEx.Types.index()],
          t()
        ) :: {:ok, t(), [term()]}
  def install_snapshot({snap_idx, snap_term} = idx_term, mac_mod, live_indexes, state) do
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

    new_state = %{
      state
      | current_snapshot: idx_term,
        snapshot_state: nil,
        entries: new_entries,
        range: new_range,
        live_indexes: live_indexes,
        next_index: snap_idx + 1
    }

    {:ok, new_state, []}
  end

  @doc """
  Recover snapshot state from disk.
  """
  @spec recover_snapshot(t()) :: term() | nil
  def recover_snapshot(%__MODULE__{snapshot_state: ss}), do: ss

  @doc """
  Get the current snapshot index and term.
  """
  @spec snapshot_index_term(t()) :: {RaftEx.Types.index(), RaftEx.Types.term_num()} | nil
  def snapshot_index_term(%__MODULE__{current_snapshot: cs}), do: cs

  # ---------------------------------------------------------------------------
  # Checkpoint and Release Cursor
  # ---------------------------------------------------------------------------

  @doc """
  Update the release cursor for snapshot management.
  """
  @spec update_release_cursor(
          RaftEx.Types.index(),
          atom(),
          {module(), term()},
          term(),
          t()
        ) :: {t(), [term()]}
  def update_release_cursor(idx, cluster, {mac_ver, mac_mod}, mac_state, state) do
    # In a full implementation, this would coordinate with the cluster
    # to determine if a snapshot can be released
    {state, []}
  end

  @doc """
  Create a checkpoint at the given index.
  """
  @spec checkpoint(
          RaftEx.Types.index(),
          atom(),
          map(),
          term(),
          t()
        ) :: {t(), [term()]}
  def checkpoint(idx, cluster, mac_ctx, mac_state, state) do
    # Persist checkpoint metadata
    LogMeta.store(state.meta_name, state.cfg.uid, :last_applied, idx)

    {state, []}
  end

  @doc """
  Promote a checkpoint to be the new recovery point.
  """
  @spec promote_checkpoint(RaftEx.Types.index(), t()) :: {t(), [term()]}
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
  Check if the log can accept writes.
  """
  @spec can_write?(t()) :: boolean()
  def can_write?(%__MODULE__{}), do: true

  @doc """
  Check if an entry exists in the log.
  """
  @spec exists({RaftEx.Types.index(), RaftEx.Types.term_num()}, t()) :: {boolean(), t()}
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
      type: RaftEx.Log,
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
  Write the cluster configuration to disk.
  """
  @spec write_config(map(), t()) :: :ok
  def write_config(config, %__MODULE__{cfg: %{uid: uid} = cfg}) do
    dir = server_data_dir(cfg.system_config.data_dir, uid)
    config_path = Path.join(dir, "config")
    clean = Map.drop(config, [:parent, :counter, :has_changed, :system_config])
    # Use :erlang.term_to_binary for reliable serialization
    RaftEx.Lib.write_file(config_path, :erlang.term_to_binary(clean), false)
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
    RaftEx.Lib.recursive_delete(dir)
    :ok
  end

  @doc """
  Release resources (file handles, etc.).
  """
  @spec release_resources(non_neg_integer(), atom(), t()) :: t()
  def release_resources(max_open, access_pattern, state), do: state

  @doc """
  Handle a log event (e.g., compaction notification).
  """
  @spec handle_event(term(), t()) :: {t(), [term()]}
  def handle_event(evt, %__MODULE__{} = state), do: {state, []}

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
        case RaftEx.LogSnapshot.recover(snapshot_dir) do
          {:ok, meta, mac_state} ->
            snap_idx = Map.get(meta, :index, 0)
            snap_term = Map.get(meta, :term, 0)
            {snap_idx, snap_term, mac_state}

          _ ->
            {0, 0, nil}
        end

      _ ->
        {0, 0, nil}
    end
  end
end
