defmodule RaftEx.Infra.Persistence.LogWal do
  @moduledoc """
  Write-ahead log process for RaftEx.

  Accepts batched append requests from server processes and writes them
  durably before acknowledging. The WAL provides:

  - **Batched writes**: Multiple entries are grouped and written together
  - **Configurable fsync**: Supports `:datasync`, `:fsync`, or `:none`
  - **Recovery**: Replays WAL entries on startup
  - **Truncation**: Removes entries up to a given index after snapshotting
  - **Size limits**: Rotates when WAL exceeds configured size

  ## Architecture

  A single WAL process serves every server in a system, but each server
  (identified by its `:uid`) owns its own WAL file, its own buffer and its own
  index range.  Servers may not share a WAL file: entries are only indexed by
  their per-server log index, so interleaving two servers in one file would
  make recovery impossible.

  Files live at `<wal_data_dir>/<uid>/wal.dat`, which is exactly where
  `RaftEx.Infra.Persistence.Log` looks when it recovers on startup.

  The WAL maintains an in-memory buffer of pending entries per file. When a
  write request arrives, entries are added to the buffer. A flush is triggered
  when:

  - The buffer reaches `wal_max_batch_size`
  - A synchronous write is requested
  - A periodic timer fires

  Entries are written to a WAL file with a simple binary format:
  ```
  <<magic::32, version::8, entry_count::32, entries::binary, crc::32>>
  ```

  ## Integration

  Started by `RaftEx.Infra.Persistence.LogWalSupervisor` as part of the system supervision tree.
  The Log module sends `{:append, uid, entries}` messages and receives
  `{:wal_written, first_index, last_index}` notifications.
  """

  use GenServer
  require Logger

  alias RaftEx.Infra.Observability.Metrics

  # "RAWL"
  @magic 0x5241574C
  # v2 adds the byte size of the entries blob to the header, so recovery can
  # read it exactly.  v1 only stored a count and could not be read back.
  @version 2
  # crc32
  @footer_size 4

  # Upper bound for a single WAL record payload. Records declaring more are
  # treated as corrupt and stop recovery instead of allocating unbounded memory.
  @max_blob_size 64_000_000

  # Default flush interval in milliseconds
  @default_flush_interval 50

  # ---------------------------------------------------------------------------
  # State
  # ---------------------------------------------------------------------------

  defstruct [
    :config,
    :files,
    :flush_timer,
    flush_scheduled?: false
  ]

  @type file_state :: %{
          required(:path) => Path.t(),
          required(:fd) => :file.io_device() | nil,
          required(:buffer) => [
            {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}
          ],
          required(:buffer_size) => non_neg_integer(),
          required(:pending_sync) => [
            {term(), RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index()}
          ],
          required(:pending_writes) => [{pid(), reference()}],
          required(:first_index) => RaftEx.Domain.Types.index(),
          required(:last_index) => RaftEx.Domain.Types.index(),
          required(:file_size) => non_neg_integer()
        }

  @type t :: %__MODULE__{
          config: map(),
          files: %{optional(term()) => file_state()},
          flush_timer: reference() | nil,
          flush_scheduled?: boolean()
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Start the WAL GenServer.

  ## Options

  - `:names` - Map of process names including `:wal`
  - `:dir` / `:data_dir` - Base directory; one `wal.dat` per server uid is kept
    under `<dir>/<uid>/`
  - `:wal_max_size_bytes` - Maximum WAL file size before rotation
  - `:wal_max_batch_size` - Maximum entries per batch write
  - `:wal_sync_method` - `:datasync`, `:fsync`, or `:none`
  - `:wal_compute_checksums` - Whether to compute CRC32 checksums

  The supervisor-style aliases `:max_size_bytes`, `:max_batch_size`,
  `:sync_method`, `:compute_checksums`, and `:max_entries` are also honored;
  `normalize_config/1` resolves both styles to the canonical `wal_*` keys.
  """
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(%{names: %{wal: name}} = config) do
    GenServer.start_link(__MODULE__, config, name: name)
  end

  @doc """
  Resolve a WAL config to its canonical `wal_*` keys.

  Accepts both the canonical keys (`:wal_sync_method`,
  `:wal_compute_checksums`, `:wal_max_batch_size`, `:wal_max_size_bytes`,
  `:wal_max_entries`) and the supervisor-style aliases (`:sync_method`,
  `:compute_checksums`, `:max_batch_size`, `:max_size_bytes`,
  `:max_entries`). Canonical keys win when both are present. All other keys
  (`:names`, `:dir`, `:data_dir`, `:system`, ...) pass through unchanged.
  """
  @spec normalize_config(map()) :: map()
  def normalize_config(config) when is_map(config) do
    Map.merge(config, %{
      wal_sync_method:
        Map.get(config, :wal_sync_method, Map.get(config, :sync_method, :datasync)),
      wal_compute_checksums:
        Map.get(config, :wal_compute_checksums, Map.get(config, :compute_checksums, true)),
      wal_max_batch_size:
        Map.get(config, :wal_max_batch_size, Map.get(config, :max_batch_size, 8192)),
      wal_max_size_bytes:
        Map.get(config, :wal_max_size_bytes, Map.get(config, :max_size_bytes, 256_000_000)),
      wal_max_entries: Map.get(config, :wal_max_entries, Map.get(config, :max_entries, nil))
    })
  end

  @doc """
  Append entries to a server's WAL.

  Returns `{:ok, first_index, last_index}` when entries are durably written.
  """
  @spec append(
          atom(),
          term(),
          [{RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}],
          timeout()
        ) ::
          {:ok, RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index()} | {:error, term()}
  def append(wal_name, uid, entries, timeout \\ 5_000) do
    GenServer.call(wal_name, {:append, uid, entries}, timeout)
  end

  @doc """
  Asynchronously append entries to a server's WAL.

  The caller will receive `{:wal_written, first_index, last_index}` when complete.
  """
  @spec append_async(
          atom(),
          term(),
          [{RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}]
        ) :: :ok
  def append_async(wal_name, uid, entries) do
    GenServer.cast(wal_name, {:append_async, uid, entries, self()})
  end

  @doc """
  Truncate a server's WAL, removing all entries up to and including `index`.

  Used after installing a snapshot to discard old entries.
  """
  @spec truncate(atom(), term(), RaftEx.Domain.Types.index(), timeout()) ::
          :ok | {:error, term()}
  def truncate(wal_name, uid, index, timeout \\ 5_000) do
    GenServer.call(wal_name, {:truncate, uid, index}, timeout)
  end

  @doc """
  Truncate a server's WAL, removing all entries at or after `from_index`.

  Used to discard a conflicting suffix after an AppendEntries rejection.
  """
  @spec truncate_from(atom(), term(), RaftEx.Domain.Types.index(), timeout()) ::
          :ok | {:error, term()}
  def truncate_from(wal_name, uid, from_index, timeout \\ 5_000) do
    GenServer.call(wal_name, {:truncate_from, uid, from_index}, timeout)
  end

  @doc """
  Atomically replace a server's WAL suffix starting at `from_index` with
  `new_entries`.

  Retains entries with indexes below `from_index` and appends `new_entries`
  in one temp-write/rename step, so a crash leaves either the complete old
  file or the complete new file. Used for follower conflict replacement.
  """
  @spec replace_from(atom(), term(), RaftEx.Domain.Types.index(), list(), timeout()) ::
          :ok | {:error, term()}
  def replace_from(wal_name, uid, from_index, new_entries, timeout \\ 5_000) do
    GenServer.call(wal_name, {:replace_from, uid, from_index, new_entries}, timeout)
  end

  @doc """
  Get a server's current WAL state.

  Returns `{first_index, last_index, buffer_size}`.
  """
  @spec info(atom(), term()) ::
          {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index(), non_neg_integer()}
  def info(wal_name, uid) do
    GenServer.call(wal_name, {:info, uid})
  end

  @doc """
  Read entries from a server's WAL buffer (in-memory only).

  Returns entries in the range `[from_index, to_index]`.
  """
  @spec read(atom(), term(), RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index()) ::
          [{RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}]
  def read(wal_name, uid, from_index, to_index) do
    GenServer.call(wal_name, {:read, uid, from_index, to_index})
  end

  @doc """
  Recover entries from a server's WAL file on startup.

  `config` may be `%{data_dir: dir}` pointing at the *server* directory (as
  `Log.init/1` does) or a full WAL config containing `:dir`/`:data_dir` plus a
  `:uid`.

  Returns `{first_index, last_index, entries}`.
  """
  @spec recover(map()) :: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index(), list()}
  def recover(config) do
    server_dir = recover_dir_path(config)
    recover_all_entries(server_dir, compute_checksums?(config))
  end

  # ---------------------------------------------------------------------------
  # GenServer Callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(config) do
    Process.flag(:trap_exit, true)

    normalized = normalize_config(config)
    wal_dir = Map.get(normalized, :dir, Map.get(normalized, :data_dir, "."))
    :ok = File.mkdir_p!(wal_dir)

    state = %__MODULE__{
      config: normalized,
      files: %{},
      flush_timer: start_flush_timer()
    }

    Logger.info("RaftEx.Infra.Persistence.LogWal: started, base directory #{wal_dir}")

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:append, uid, entries}, from, state) do
    {file, state} = ensure_file(uid, state)

    cond do
      entries == [] ->
        state = put_file(uid, file, state)
        reply_or_noreply(from, {:ok, file.first_index, file.last_index}, state)

      reply_target?(from) ->
        prior_size = file.buffer_size
        file = add_to_buffer(entries, file)
        {first, last} = entry_range(entries)
        file = %{file | pending_sync: [{from, first, last} | file.pending_sync]}

        cond do
          file.buffer_size >= max_batch_size(state.config) ->
            file = finish_flush(file, state.config)
            {:noreply, put_file(uid, file, state)}

          prior_size > 0 or state.flush_scheduled? or queued_appends?() ->
            {:noreply, schedule_flush(put_file(uid, file, state))}

          true ->
            # No other append is waiting, so there is nothing to group with.
            # Flush now instead of paying for a self-message round trip.
            file = finish_flush(file, state.config)
            {:noreply, put_file(uid, file, state)}
        end

      true ->
        # A direct callback invocation with no real caller: buffer the entries
        # without scheduling a group commit.
        file = add_to_buffer(entries, file)
        file = flush_if_full(file, state.config)
        state = put_file(uid, file, state)
        {:reply, {:ok, file.first_index, file.last_index}, state}
    end
  end

  def handle_call({:truncate, uid, index}, _from, state) do
    case Map.fetch(state.files, uid) do
      {:ok, file} ->
        case apply_truncate_prefix(file, index, state.config) do
          {:ok, file} ->
            {:reply, :ok, put_file(uid, file, state)}

          {:error, reason, file} ->
            {:reply, {:error, reason}, put_file(uid, file, state)}
        end

      :error ->
        {:reply, :ok, state}
    end
  end

  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def handle_call({:truncate_from, uid, from_index}, _from, state) do
    case Map.fetch(state.files, uid) do
      {:ok, file} ->
        case apply_truncate_from(file, from_index, state.config) do
          {:ok, file} ->
            {:reply, :ok, put_file(uid, file, state)}

          {:error, reason, file} ->
            {:reply, {:error, reason}, put_file(uid, file, state)}
        end

      :error ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:replace_from, uid, from_index, new_entries}, _from, state) do
    {file, state} = ensure_file(uid, state)

    case apply_replace_from(file, from_index, new_entries, state.config) do
      {:ok, file} ->
        {:reply, :ok, put_file(uid, file, state)}

      {:error, reason, file} ->
        {:reply, {:error, reason}, put_file(uid, file, state)}
    end
  end

  def handle_call({:info, uid}, _from, state) do
    reply =
      case Map.fetch(state.files, uid) do
        {:ok, file} -> {file.first_index, file.last_index, file.buffer_size}
        :error -> {0, 0, 0}
      end

    {:reply, reply, state}
  end

  def handle_call(:overview, _from, state) do
    files = state.files
    buffers = Enum.map(files, fn {_uid, file} -> file.buffer_size end)
    indexes = Map.values(files)

    reply = %{
      files: map_size(files),
      first_index: indexes |> Enum.map(& &1.first_index) |> Enum.min(fn -> 0 end),
      last_index: indexes |> Enum.map(& &1.last_index) |> Enum.max(fn -> 0 end),
      buffer_size: Enum.sum(buffers),
      pending_writes: indexes |> Enum.flat_map(& &1.pending_writes) |> Enum.count()
    }

    {:reply, reply, state}
  end

  def handle_call({:read, uid, from_index, to_index}, _from, state) do
    entries =
      case Map.fetch(state.files, uid) do
        {:ok, file} ->
          file.buffer
          |> Enum.filter(fn {idx, _, _} -> idx >= from_index and idx <= to_index end)
          |> Enum.reverse()

        :error ->
          []
      end

    {:reply, entries, state}
  end

  @impl GenServer
  def handle_cast({:append_async, uid, entries, reply_to}, state) do
    {file, state} = ensure_file(uid, state)
    file = add_to_buffer(entries, file)
    file = maybe_flush_async(file, reply_to, state.config)
    state = put_file(uid, file, state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:flush_timeout, state) do
    files =
      Map.new(state.files, fn {uid, file} ->
        {uid, finish_flush(file, state.config)}
      end)

    {:noreply, %{state | files: files, flush_timer: start_flush_timer()}}
  end

  def handle_info(:group_commit, state) do
    files =
      Map.new(state.files, fn {uid, file} ->
        {uid, finish_flush(file, state.config)}
      end)

    {:noreply, %{state | files: files, flush_scheduled?: false}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    files =
      Map.new(state.files, fn {uid, file} ->
        pending_writes = Enum.reject(file.pending_writes, fn {p, _} -> p == pid end)
        {uid, %{file | pending_writes: pending_writes}}
      end)

    {:noreply, %{state | files: files}}
  end

  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.files, fn {_uid, file} ->
      file = finish_flush(file, state.config)

      if file.fd do
        :file.close(file.fd)
      end
    end)

    Logger.info("RaftEx.Infra.Persistence.LogWal: shutting down")

    :ok
  end

  # ---------------------------------------------------------------------------
  # File lifecycle
  # ---------------------------------------------------------------------------

  # Return the WAL file for `uid`, opening (and recovering) it on first use.
  defp ensure_file(uid, state) do
    case Map.fetch(state.files, uid) do
      {:ok, file} ->
        {file, state}

      :error ->
        dir = Map.get(state.config, :dir, Map.get(state.config, :data_dir, "."))
        server_dir = Path.join(dir, to_string(uid))
        path = Path.join(server_dir, "wal.dat")
        :ok = File.mkdir_p!(Path.dirname(path))

        checksums = compute_checksums?(state.config)
        {first_index, last_index, _entries} = recover_all_entries(server_dir, checksums)

        # Entries already in the file must not be buffered again: they are
        # recovered into the server's log state by `Log.init/1`, and rewriting
        # them here would duplicate them on every restart.
        {:ok, fd} = :file.open(path, [:append, :binary, :raw])
        {:ok, file_size} = :file.position(fd, :eof)

        file = %{
          path: path,
          fd: fd,
          buffer: [],
          buffer_size: 0,
          pending_sync: [],
          pending_writes: [],
          first_index: first_index,
          last_index: last_index,
          file_size: file_size,
          rotation_seq: next_rotation_seq(server_dir)
        }

        {file, %{state | files: Map.put(state.files, uid, file)}}
    end
  end

  defp put_file(uid, file, state), do: %{state | files: Map.put(state.files, uid, file)}

  # ---------------------------------------------------------------------------
  # Buffering and flushing
  # ---------------------------------------------------------------------------

  defp add_to_buffer([], file), do: file

  defp add_to_buffer(entries, file) do
    # The buffer is kept newest-first so appending a batch is a single reverse
    # into the existing list instead of `buffer ++ entries`, which is quadratic
    # in the number of buffered entries.
    {last_idx, _, _} = List.last(entries)

    %{
      file
      | buffer: Enum.reverse(entries, file.buffer),
        buffer_size: file.buffer_size + length(entries),
        last_index: last_idx
    }
  end

  defp entry_range(entries) do
    {first, _, _} = hd(entries)
    {last, _, _} = List.last(entries)
    {first, last}
  end

  defp flush_if_full(file, config) do
    if file.buffer_size >= max_batch_size(config) do
      finish_flush(file, config)
    else
      file
    end
  end

  defp max_batch_size(config),
    do: Map.get(config, :wal_max_batch_size, Map.get(config, :max_batch_size, 8192))

  defp compute_checksums?(config),
    do: Map.get(config, :wal_compute_checksums, Map.get(config, :compute_checksums, true))

  # True when other messages are already waiting: another append is
  # likely queued behind this one, so defer the sync and let the scheduled
  # flush group them. Uses the O(1) queue-length counter instead of copying
  # and scanning the whole mailbox on every append (11.5).
  defp queued_appends? do
    case Process.info(self(), :message_queue_len) do
      {:message_queue_len, n} -> n > 0
      _ -> false
    end
  end

  defp schedule_flush(%{flush_scheduled?: true} = state), do: state

  defp schedule_flush(state) do
    send(self(), :group_commit)
    %{state | flush_scheduled?: true}
  end

  # Flush one file, acknowledge its queued callers, and clear the pending
  # lists. On failure the buffer is left in place so a later flush can retry,
  # but the callers are told that the write did not succeed.
  defp finish_flush(file, config) do
    case flush_buffer(file, config) do
      {:ok, file} ->
        ack_sync(file, :ok)
        notify_pending_writes(file)
        %{file | pending_sync: [], pending_writes: []}

      {:error, reason, file} ->
        Logger.error("RaftEx.Infra.Persistence.LogWal: WAL flush failed: #{inspect(reason)}")

        Metrics.inc(:wal_flush_errors)
        ack_sync(file, {:error, reason})
        %{file | pending_sync: []}
    end
  end

  defp ack_sync(file, result) do
    Enum.each(file.pending_sync, fn {from, first, last} ->
      reply = if result == :ok, do: {:ok, first, last}, else: result
      GenServer.reply(from, reply)
    end)
  end

  defp reply_target?({pid, _ref}) when is_pid(pid), do: true
  defp reply_target?(_), do: false

  defp reply_or_noreply(from, reply, state) do
    if reply_target?(from) do
      GenServer.reply(from, reply)
      {:noreply, state}
    else
      {:reply, reply, state}
    end
  end

  defp maybe_flush_async(file, reply_to, config) do
    if file.buffer_size >= max_batch_size(config) do
      file = finish_flush(file, config)
      send(reply_to, {:wal_written, file.first_index, file.last_index})
      file
    else
      # Monitor the process in case it crashes
      ref = Process.monitor(reply_to)
      pending_writes = [{reply_to, ref} | file.pending_writes]
      %{file | pending_writes: pending_writes}
    end
  end

  defp flush_buffer(%{buffer: []} = file, _config), do: {:ok, file}

  defp flush_buffer(file, config) do
    entries = Enum.reverse(file.buffer)
    entries_binary = :erlang.term_to_binary(entries)
    start = System.monotonic_time(:millisecond)

    case write_entries_binary(entries_binary, file.buffer_size, file, config) do
      {:ok, file} ->
        case sync_wal(file, config) do
          :ok ->
            latency = System.monotonic_time(:millisecond) - start
            Metrics.inc(:wal_flushes)
            Metrics.inc(:wal_bytes_written, byte_size(entries_binary))
            Metrics.inc(:wal_segment_writes)
            Metrics.observe(:wal_flush_latency_ms, latency)

            {:ok, maybe_rotate(%{file | buffer: [], buffer_size: 0}, config)}

          {:error, reason} ->
            {:error, reason, file}
        end

      {:error, reason, file} ->
        {:error, reason, file}
    end
  end

  defp write_entries_binary(entries_binary, count, file, config) do
    compute_checksums = compute_checksums?(config)

    # Build WAL record from the already-serialized batch.
    blob_size = byte_size(entries_binary)

    header =
      <<@magic::32-unsigned, @version::8-unsigned, count::32-unsigned, blob_size::32-unsigned>>

    data =
      if compute_checksums do
        crc = :erlang.crc32([header, entries_binary])
        footer = <<crc::32-unsigned>>
        [header, entries_binary, footer]
      else
        [header, entries_binary]
      end

    data_size = :erlang.iolist_size(data)

    # Write to file
    case :file.write(file.fd, data) do
      :ok ->
        {:ok, %{file | file_size: file.file_size + data_size}}

      {:error, reason} ->
        Logger.error(
          "RaftEx.Infra.Persistence.LogWal: failed to write WAL entries: #{inspect(reason)}"
        )

        {:error, reason, file}
    end
  end

  defp sync_wal(file, config) do
    sync_method = Map.get(config, :wal_sync_method, Map.get(config, :sync_method, :datasync))

    case sync_method do
      :fsync ->
        Metrics.inc(:wal_fsyncs)
        :file.sync(file.fd)

      :datasync ->
        Metrics.inc(:wal_fsyncs)
        :file.datasync(file.fd)

      :none ->
        :ok

      _ ->
        Metrics.inc(:wal_fsyncs)
        :file.datasync(file.fd)
    end
  end

  defp notify_pending_writes(file) do
    Enum.each(file.pending_writes, fn {pid, _ref} ->
      send(pid, {:wal_written, file.first_index, file.last_index})
    end)
  end

  defp apply_truncate_prefix(file, index, config) do
    compute_checksums = compute_checksums?(config)
    _ = sync_wal(file, config)
    server_dir = Path.dirname(file.path)
    {_first, _last, disk_entries} = recover_all_entries(server_dir, compute_checksums)

    retained =
      Enum.reject(
        disk_entries ++ Enum.reverse(file.buffer),
        fn {idx, _, _} -> idx <= index end
      )

    Logger.info(
      "RaftEx.Infra.Persistence.LogWal: truncated to index #{index}, " <>
        "retaining #{length(retained)} entries"
    )

    case rewrite_wal_file_atomic(file, retained, config) do
      {:ok, file} ->
        :ok = delete_rotated_files(server_dir)

        {first, last} =
          case retained do
            [] -> {index + 1, index}
            _ -> {hd(retained) |> elem(0), List.last(retained) |> elem(0)}
          end

        file = %{file | first_index: first, last_index: last}
        ack_sync(file, :ok)
        notify_pending_writes(file)
        {:ok, %{file | pending_sync: [], pending_writes: []}}

      {:error, reason, file} ->
        {:error, reason, file}
    end
  end

  defp apply_truncate_from(file, from_index, config) do
    compute_checksums = compute_checksums?(config)
    _ = sync_wal(file, config)
    server_dir = Path.dirname(file.path)
    {_first, _last, disk_entries} = recover_all_entries(server_dir, compute_checksums)

    retained =
      Enum.reject(
        disk_entries ++ Enum.reverse(file.buffer),
        fn {idx, _, _} -> idx >= from_index end
      )

    case rewrite_wal_file_atomic(file, retained, config) do
      {:ok, file} ->
        :ok = delete_rotated_files(server_dir)

        {first, last} =
          case retained do
            [] -> {from_index, from_index - 1}
            _ -> {hd(retained) |> elem(0), List.last(retained) |> elem(0)}
          end

        file = %{file | first_index: first, last_index: last}
        ack_sync(file, :ok)
        notify_pending_writes(file)
        {:ok, %{file | pending_sync: [], pending_writes: []}}

      {:error, reason, file} ->
        {:error, reason, file}
    end
  end

  defp apply_replace_from(file, from_index, new_entries, config) do
    compute_checksums = compute_checksums?(config)
    _ = sync_wal(file, config)
    server_dir = Path.dirname(file.path)
    {_first, _last, disk_entries} = recover_all_entries(server_dir, compute_checksums)

    retained =
      Enum.reject(
        disk_entries ++ Enum.reverse(file.buffer),
        fn {idx, _, _} -> idx >= from_index end
      )

    combined = retained ++ new_entries

    case rewrite_wal_file_atomic(file, combined, config) do
      {:ok, file} ->
        :ok = delete_rotated_files(server_dir)

        {first, last} =
          case combined do
            [] -> {from_index, from_index - 1}
            _ -> {hd(combined) |> elem(0), List.last(combined) |> elem(0)}
          end

        file = %{file | first_index: first, last_index: last}
        ack_sync(file, :ok)
        notify_pending_writes(file)
        {:ok, %{file | pending_sync: [], pending_writes: []}}

      {:error, reason, file} ->
        {:error, reason, file}
    end
  end

  # Write `entries` to a temp file, sync it, atomically rename it over the
  # live WAL, and sync the containing directory. The original fd is kept open
  # until the rename succeeds, so a crash leaves either the complete old file
  # or the complete new file. Returns `{:ok, new_file}` with a reopened
  # append-mode fd, or `{:error, reason, file}` with the original untouched.
  defp rewrite_wal_file_atomic(file, entries, config) do
    compute_checksums = compute_checksums?(config)
    temp_path = file.path <> ".tmp"

    with {:ok, temp_fd} <- :file.open(temp_path, [:write, :binary, :raw]),
         {:ok, bytes_written} <- write_entries_to_fd(entries, temp_fd, compute_checksums),
         :ok <- sync_fd(temp_fd),
         :ok <- :file.close(temp_fd),
         :ok <- :file.rename(temp_path, file.path),
         :ok <- sync_dir(file.path),
         :ok <- close_original_fd(file.fd),
         {:ok, fd} <- :file.open(file.path, [:append, :binary, :raw]),
         {:ok, file_size} <- :file.position(fd, :eof) do
      Metrics.inc(:wal_flushes)
      Metrics.inc(:wal_bytes_written, bytes_written)

      {:ok,
       %{
         file
         | fd: fd,
           file_size: file_size,
           buffer: [],
           buffer_size: 0
       }}
    else
      {:error, reason} ->
        _ = File.rm(temp_path)
        {:error, reason, file}

      _other ->
        _ = File.rm(temp_path)
        {:error, :rewrite_failed, file}
    end
  end

  defp write_entries_to_fd(entries, fd, compute_checksums) do
    entries_binary = :erlang.term_to_binary(entries)
    blob_size = byte_size(entries_binary)
    count = length(entries)

    header =
      <<@magic::32-unsigned, @version::8-unsigned, count::32-unsigned, blob_size::32-unsigned>>

    data =
      if compute_checksums do
        crc = :erlang.crc32([header, entries_binary])
        [header, entries_binary, <<crc::32-unsigned>>]
      else
        [header, entries_binary]
      end

    case :file.write(fd, data) do
      :ok -> {:ok, :erlang.iolist_size(data)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp sync_fd(fd) do
    case :file.sync(fd) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  catch
    _, reason -> {:error, reason}
  end

  defp close_original_fd(nil), do: :ok

  defp close_original_fd(fd) do
    try do
      case :file.close(fd) do
        :ok -> :ok
        # Already closed (e.g. after an fd-level I/O failure): nothing to leak.
        {:error, :einval} -> :ok
        {:error, reason} -> {:error, reason}
      end
    catch
      _, reason -> {:error, reason}
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
          # OTP's file driver refuses to open directories, so a portable
          # directory fsync is unavailable. The rename itself remains atomic;
          # only the rename's durability across power loss is best-effort.
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    catch
      _, reason -> {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery
  # ---------------------------------------------------------------------------

  defp recover_dir_path(%{uid: uid} = config) do
    dir = Map.get(config, :dir, Map.get(config, :data_dir, "."))
    Path.join(dir, to_string(uid))
  end

  defp recover_dir_path(config) do
    Map.fetch!(config, :data_dir)
  end

  @doc false
  def max_size_bytes(config) do
    Map.get(config, :wal_max_size_bytes, Map.get(config, :max_size_bytes, 256_000_000))
  end

  defp rotated_seq(path) do
    base = Path.basename(path)

    case Regex.run(~r/^wal\.(\d+)\.dat$/, base, capture: :all_but_first) do
      [seq] ->
        case Integer.parse(seq) do
          {n, ""} -> {:ok, n}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp list_wal_files(server_dir) do
    case File.ls(server_dir) do
      {:ok, names} ->
        rotated =
          names
          |> Enum.map(&Path.join(server_dir, &1))
          |> Enum.filter(&(rotated_seq(&1) != :error))
          |> Enum.sort_by(fn path ->
            {:ok, n} = rotated_seq(path)
            n
          end)

        active = Path.join(server_dir, "wal.dat")

        paths =
          if File.exists?(active) do
            rotated ++ [active]
          else
            rotated
          end

        {:ok, paths}

      {:error, _} ->
        {:ok, []}
    end
  end

  defp next_rotation_seq(server_dir) do
    {:ok, paths} = list_wal_files(server_dir)

    seqs =
      for path <- paths,
          {:ok, n} <- [rotated_seq(path)],
          do: n

    case seqs do
      [] -> 1
      _ -> Enum.max(seqs) + 1
    end
  end

  defp recover_all_entries(server_dir, compute_checksums) do
    case list_wal_files(server_dir) do
      {:ok, []} ->
        {0, 0, []}

      {:ok, paths} ->
        all =
          Enum.flat_map(paths, fn path ->
            {_f, _l, entries} = do_recover_file(path, compute_checksums)
            entries
          end)

        recovered_range(all)
    end
  end

  defp delete_rotated_files(server_dir) do
    {:ok, paths} = list_wal_files(server_dir)

    Enum.each(paths, fn path ->
      if rotated_seq(path) != :error, do: File.rm(path)
    end)

    :ok
  end

  defp maybe_rotate(file, config) do
    max = max_size_bytes(config)

    if is_integer(max) and max > 0 and file.file_size >= max do
      rotate_active_file(file)
    else
      file
    end
  end

  defp rotate_active_file(%{path: path, fd: fd} = file) do
    seq = Map.get(file, :rotation_seq, 1)
    server_dir = Path.dirname(path)
    rotated_path = Path.join(server_dir, "wal.#{seq}.dat")

    with :ok <- sync_fd(fd),
         :ok <- close_original_fd(fd),
         :ok <- File.rename(path, rotated_path),
         :ok <- sync_dir(path),
         {:ok, new_fd} <- :file.open(path, [:append, :binary, :raw]),
         {:ok, file_size} <- :file.position(new_fd, :eof) do
      Logger.info("RaftEx.Infra.Persistence.LogWal: rotated to #{Path.basename(rotated_path)}")

      %{
        file
        | fd: new_fd,
          file_size: file_size,
          buffer: [],
          buffer_size: 0,
          rotation_seq: seq + 1
      }
    else
      {:error, reason} ->
        Metrics.inc(:wal_rotation_errors)
        Logger.error("RaftEx.Infra.Persistence.LogWal: rotation failed: #{inspect(reason)}")
        reopen_after_failed_rotate(file)
    end
  catch
    _, reason ->
      Metrics.inc(:wal_rotation_errors)
      Logger.error("RaftEx.Infra.Persistence.LogWal: rotation failed: #{inspect(reason)}")
      reopen_after_failed_rotate(file)
  end

  defp reopen_after_failed_rotate(%{path: path} = file) do
    case :file.open(path, [:append, :binary, :raw]) do
      {:ok, fd} ->
        {:ok, size} = :file.position(fd, :eof)
        %{file | fd: fd, file_size: size}

      _ ->
        file
    end
  end

  defp do_recover_file(wal_file, compute_checksums) do
    case :prim_file.read_file_info(wal_file) do
      {:ok, _info} ->
        case :file.open(wal_file, [:read, :binary, :raw]) do
          {:ok, fd} ->
            entries =
              fd
              |> do_recover(compute_checksums, [])
              |> Enum.reverse()

            :file.close(fd)
            recovered_range(entries)

          _ ->
            {0, 0, []}
        end

      _ ->
        {0, 0, []}
    end
  end

  defp recovered_range([]), do: {0, 0, []}

  defp recovered_range(recovered) do
    {first_idx, _, _} = hd(recovered)
    {last_idx, _, _} = List.last(recovered)
    {first_idx, last_idx, recovered}
  end

  defp do_recover(fd, compute_checksums, acc) do
    case :file.read(fd, 5) do
      {:ok, <<@magic::32-unsigned, version::8-unsigned>>} ->
        do_recover_records(fd, version, compute_checksums, acc)

      _ ->
        # Not a WAL file (or a truncated header): stop.
        acc
    end
  end

  defp do_recover_records(fd, @version, compute_checksums, acc) do
    case read_exact(fd, 8) do
      {:ok, <<count::32-unsigned, blob_size::32-unsigned>>} ->
        cond do
          blob_size > @max_blob_size ->
            # Implausible length: stop instead of allocating.
            acc

          true ->
            header =
              <<@magic::32-unsigned, @version::8-unsigned, count::32-unsigned,
                blob_size::32-unsigned>>

            case read_exact(fd, blob_size) do
              {:ok, entries_binary} ->
                recover_verified_record(fd, compute_checksums, header, entries_binary, acc)

              _ ->
                # Truncated payload (:eof or short read): stop, keep prior records.
                acc
            end
        end

      _ ->
        # Truncated header: stop.
        acc
    end
  end

  # v1 only stored a count, so the blob size had to be estimated and the
  # checksum footer was effectively unreadable.  There is no reliable way to
  # decode those files; stop instead of returning half a log.
  defp do_recover_records(_fd, _version, _compute_checksums, acc), do: acc

  defp recover_verified_record(fd, compute_checksums, header, entries_binary, acc) do
    if compute_checksums do
      case read_exact(fd, @footer_size) do
        {:ok, <<expected_crc::32-unsigned>>} ->
          actual_crc = :erlang.crc32([header, entries_binary])

          if actual_crc == expected_crc do
            case decode(entries_binary) do
              {:ok, decoded} -> do_recover(fd, compute_checksums, Enum.reverse(decoded, acc))
              :error -> acc
            end
          else
            # Checksum mismatch: stop at the last valid record.
            acc
          end

        _ ->
          acc
      end
    else
      case decode(entries_binary) do
        {:ok, decoded} -> do_recover(fd, compute_checksums, Enum.reverse(decoded, acc))
        :error -> acc
      end
    end
  end

  defp decode(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  catch
    _, _ -> :error
  end

  # Read exactly `n` bytes unless the file ends first. Returns
  # `{:ok, binary}` only when all `n` bytes were read; `:eof` and short
  # reads stop recovery. `n = 0` returns an empty binary without reading.
  defp read_exact(_fd, 0), do: {:ok, <<>>}

  defp read_exact(fd, n) when is_integer(n) and n > 0 do
    read_exact(fd, n, [])
  end

  defp read_exact(_fd, 0, acc), do: {:ok, :erlang.iolist_to_binary(Enum.reverse(acc))}

  defp read_exact(fd, remaining, acc) when remaining > 0 do
    case :file.read(fd, remaining) do
      {:ok, bin} when is_binary(bin) ->
        got = byte_size(bin)

        cond do
          got == remaining ->
            {:ok, :erlang.iolist_to_binary(Enum.reverse([bin | acc]))}

          got > 0 ->
            read_exact(fd, remaining - got, [bin | acc])

          true ->
            :eof
        end

      :eof ->
        :eof

      {:error, _} = err ->
        err
    end
  end

  defp start_flush_timer do
    Process.send_after(self(), :flush_timeout, @default_flush_interval)
  end

  # ---------------------------------------------------------------------------
  # Overview for metrics
  # ---------------------------------------------------------------------------

  @doc """
  Get WAL overview for metrics and monitoring.
  """
  @spec overview(atom()) :: map()
  def overview(wal_name) do
    GenServer.call(wal_name, :overview, 5_000)
  catch
    _, _ -> %{files: 0, first_index: 0, last_index: 0, buffer_size: 0, pending_writes: 0}
  end
end
