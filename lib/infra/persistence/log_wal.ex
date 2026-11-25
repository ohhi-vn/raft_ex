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

  # "RAWL"
  @magic 0x5241574C
  # v2 adds the byte size of the entries blob to the header, so recovery can
  # read it exactly.  v1 only stored a count and could not be read back.
  @version 2
  # crc32
  @footer_size 4

  # Default flush interval in milliseconds
  @default_flush_interval 50

  # ---------------------------------------------------------------------------
  # State
  # ---------------------------------------------------------------------------

  defstruct [
    :config,
    :files,
    :flush_timer
  ]

  @type file_state :: %{
          required(:path) => Path.t(),
          required(:fd) => :file.io_device() | nil,
          required(:buffer) => [
            {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}
          ],
          required(:pending_writes) => [{pid(), reference()}],
          required(:first_index) => RaftEx.Domain.Types.index(),
          required(:last_index) => RaftEx.Domain.Types.index(),
          required(:file_size) => non_neg_integer()
        }

  @type t :: %__MODULE__{
          config: map(),
          files: %{optional(term()) => file_state()},
          flush_timer: reference() | nil
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
  """
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(%{names: %{wal: name}} = config) do
    GenServer.start_link(__MODULE__, config, name: name)
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
    wal_file = recover_file_path(config)
    do_recover_file(wal_file, Map.get(config, :wal_compute_checksums, true))
  end

  # ---------------------------------------------------------------------------
  # GenServer Callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(config) do
    Process.flag(:trap_exit, true)

    wal_dir = Map.get(config, :dir, Map.get(config, :data_dir, "."))
    :ok = File.mkdir_p!(wal_dir)

    state = %__MODULE__{
      config: config,
      files: %{},
      flush_timer: start_flush_timer()
    }

    Logger.info("RaftEx.Infra.Persistence.LogWal: started, base directory #{wal_dir}")

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:append, uid, entries}, from, state) do
    {file, state} = ensure_file(uid, state)
    file = add_to_buffer(entries, file)
    file = maybe_flush(file, from, state.config)
    state = put_file(uid, file, state)
    {:reply, {:ok, file.first_index, file.last_index}, state}
  end

  def handle_call({:truncate, uid, index}, _from, state) do
    state =
      case Map.fetch(state.files, uid) do
        {:ok, file} ->
          file = do_truncate(index, file)
          put_file(uid, file, state)

        :error ->
          state
      end

    {:reply, :ok, state}
  end

  def handle_call({:truncate_from, uid, from_index}, _from, state) do
    state =
      case Map.fetch(state.files, uid) do
        {:ok, file} ->
          compute_checksums = Map.get(state.config, :wal_compute_checksums, true)
          sync_wal(file, state.config)
          {_first, _last, disk_entries} = do_recover_file(file.path, compute_checksums)

          entries =
            Enum.reject(disk_entries ++ file.buffer, fn {idx, _, _} -> idx >= from_index end)

          :ok = :file.close(file.fd)
          {:ok, fd} = :file.open(file.path, [:write, :binary, :raw, :truncate])

          file = %{file | fd: fd, file_size: 0, buffer: [], pending_writes: []}
          file = write_entries_to_wal(entries, file, state.config)
          sync_wal(file, state.config)

          {first, last} =
            case entries do
              [] -> {from_index, from_index - 1}
              _ -> {hd(entries) |> elem(0), List.last(entries) |> elem(0)}
            end

          put_file(uid, %{file | first_index: first, last_index: last}, state)

        :error ->
          state
      end

    {:reply, :ok, state}
  end

  def handle_call({:info, uid}, _from, state) do
    reply =
      case Map.fetch(state.files, uid) do
        {:ok, file} -> {file.first_index, file.last_index, length(file.buffer)}
        :error -> {0, 0, 0}
      end

    {:reply, reply, state}
  end

  def handle_call(:overview, _from, state) do
    files = state.files
    buffers = Enum.map(files, fn {_uid, file} -> length(file.buffer) end)
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
          |> Enum.sort_by(fn {idx, _, _} -> idx end)

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
        {uid, flush_buffer(file, state.config)}
      end)

    {:noreply, %{state | files: files, flush_timer: start_flush_timer()}}
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
      file = flush_buffer(file, state.config)

      if file.fd do
        sync_wal(file, state.config)
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
        path = Path.join([dir, to_string(uid), "wal.dat"])
        :ok = File.mkdir_p!(Path.dirname(path))

        checksums = Map.get(state.config, :wal_compute_checksums, true)
        {first_index, last_index, _entries} = do_recover_file(path, checksums)

        # Entries already in the file must not be buffered again: they are
        # recovered into the server's log state by `Log.init/1`, and rewriting
        # them here would duplicate them on every restart.
        {:ok, fd} = :file.open(path, [:append, :binary, :raw, :delayed_write])
        {:ok, file_size} = :file.position(fd, :eof)

        file = %{
          path: path,
          fd: fd,
          buffer: [],
          pending_writes: [],
          first_index: first_index,
          last_index: last_index,
          file_size: file_size
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
    new_buffer = file.buffer ++ entries
    {last_idx, _, _} = List.last(entries)

    %{file | buffer: new_buffer, last_index: last_idx}
  end

  defp maybe_flush(file, from, config) do
    max_batch_size = Map.get(config, :wal_max_batch_size, 8192)

    if length(file.buffer) >= max_batch_size or from != nil do
      file = flush_buffer(file, config)

      if from do
        send_reply(from, file)
      end

      file
    else
      file
    end
  end

  defp maybe_flush_async(file, reply_to, config) do
    max_batch_size = Map.get(config, :wal_max_batch_size, 8192)

    if length(file.buffer) >= max_batch_size do
      file = flush_buffer(file, config)
      send(reply_to, {:wal_written, file.first_index, file.last_index})
      file
    else
      # Monitor the process in case it crashes
      ref = Process.monitor(reply_to)
      pending_writes = [{reply_to, ref} | file.pending_writes]
      %{file | pending_writes: pending_writes}
    end
  end

  defp flush_buffer(%{buffer: []} = file, _config), do: file

  defp flush_buffer(file, config) do
    entries = file.buffer
    file = write_entries_to_wal(entries, file, config)
    sync_wal(file, config)
    notify_pending_writes(file)

    %{file | buffer: [], pending_writes: []}
  end

  defp write_entries_to_wal(entries, file, config) do
    compute_checksums = Map.get(config, :wal_compute_checksums, true)

    # Serialize entries
    entries_binary = :erlang.term_to_binary(entries)

    # Build WAL record
    count = length(entries)
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
        %{file | file_size: file.file_size + data_size}

      error ->
        Logger.error(
          "RaftEx.Infra.Persistence.LogWal: failed to write WAL entries: #{inspect(error)}"
        )

        file
    end
  end

  defp sync_wal(file, config) do
    sync_method = Map.get(config, :wal_sync_method, :datasync)

    case sync_method do
      :fsync ->
        :file.sync(file.fd)

      :datasync ->
        :file.datasync(file.fd)

      :none ->
        :ok

      _ ->
        :file.datasync(file.fd)
    end
  end

  defp notify_pending_writes(file) do
    Enum.each(file.pending_writes, fn {pid, _ref} ->
      send(pid, {:wal_written, file.first_index, file.last_index})
    end)
  end

  defp send_reply({pid, ref}, file) do
    send(pid, {:wal_written, ref, file.first_index, file.last_index})
  end

  defp send_reply(pid, file) when is_pid(pid) do
    send(pid, {:wal_written, file.first_index, file.last_index})
  end

  defp send_reply(_other, _file), do: :ok

  defp do_truncate(index, file) do
    # Remove entries up to and including index
    new_buffer = Enum.reject(file.buffer, fn {idx, _, _} -> idx <= index end)

    new_first_index =
      case new_buffer do
        [] -> index + 1
        [{first_idx, _, _} | _] -> first_idx
      end

    # Truncate WAL file
    :ok = :file.truncate(file.fd)
    {:ok, 0} = :file.position(file.fd, :bof)

    Logger.info(
      "RaftEx.Infra.Persistence.LogWal: truncated to index #{index}, " <>
        "new first_index=#{new_first_index}"
    )

    %{file | buffer: new_buffer, first_index: new_first_index, file_size: 0}
  end

  # ---------------------------------------------------------------------------
  # Recovery
  # ---------------------------------------------------------------------------

  defp recover_file_path(%{uid: uid} = config) do
    # Full WAL config: a uid selects the server's file under the base dir.
    dir = Map.get(config, :dir, Map.get(config, :data_dir, "."))
    Path.join([dir, to_string(uid), "wal.dat"])
  end

  defp recover_file_path(config) do
    # Server directory, as passed by `Log.init/1`.
    Path.join(Map.fetch!(config, :data_dir), "wal.dat")
  end

  defp do_recover_file(wal_file, compute_checksums) do
    case :prim_file.read_file_info(wal_file) do
      {:ok, _info} ->
        case :file.open(wal_file, [:read, :binary, :raw]) do
          {:ok, fd} ->
            entries = do_recover(fd, compute_checksums, [])
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
    case :file.read(fd, 8) do
      {:ok, <<_count::32-unsigned, blob_size::32-unsigned>>} ->
        {:ok, entries_binary} = :file.read(fd, blob_size)
        recover_records_with_footer(fd, compute_checksums, entries_binary, acc)

      _ ->
        acc
    end
  end

  # v1 only stored a count, so the blob size had to be estimated and the
  # checksum footer was effectively unreadable.  There is no reliable way to
  # decode those files; stop instead of returning half a log.
  defp do_recover_records(_fd, _version, _compute_checksums, acc), do: acc

  defp recover_records_with_footer(fd, compute_checksums, entries_binary, acc) do
    if compute_checksums do
      case :file.read(fd, @footer_size) do
        {:ok, <<_crc::32-unsigned>>} ->
          do_recover(fd, compute_checksums, acc ++ decode(entries_binary))

        _ ->
          acc
      end
    else
      do_recover(fd, compute_checksums, acc ++ decode(entries_binary))
    end
  end

  defp decode(binary) do
    :erlang.binary_to_term(binary)
  rescue
    ArgumentError -> []
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
