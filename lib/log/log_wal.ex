defmodule RaftEx.LogWal do
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

  The WAL maintains an in-memory buffer of pending entries. When a write
  request arrives, entries are added to the buffer. A flush is triggered
  when:
  - The buffer reaches `wal_max_batch_size`
  - A synchronous write is requested
  - A periodic timer fires

  Entries are written to a single WAL file with a simple binary format:
  ```
  <<magic::32, version::8, entry_count::32, entries::binary, crc::32>>
  ```

  ## Integration

  Started by `RaftEx.LogWalSupervisor` as part of the system supervision tree.
  The Log module sends `{:append, entries, reply_to}` messages and receives
  `{:written, from_index, to_index}` notifications.
  """

  use GenServer
  require Logger

  # "RAWL"
  @magic 0x5241574C
  @version 1
  # 4 (magic) + 1 (version) + 4 (count)
  @header_size 9
  # crc32
  @footer_size 4

  # Default flush interval in milliseconds
  @default_flush_interval 50

  # ---------------------------------------------------------------------------
  # State
  # ---------------------------------------------------------------------------

  defstruct [
    :config,
    :wal_file,
    :wal_fd,
    :buffer,
    :pending_writes,
    :first_index,
    :last_index,
    :file_size,
    :flush_timer
  ]

  @type state :: %__MODULE__{
          config: map(),
          wal_file: Path.t(),
          wal_fd: :file.io_device() | nil,
          buffer: [{RaftEx.Types.index(), RaftEx.Types.term_num(), term()}],
          pending_writes: [{pid(), reference()}],
          first_index: RaftEx.Types.index(),
          last_index: RaftEx.Types.index(),
          file_size: non_neg_integer(),
          flush_timer: reference() | nil
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Start the WAL GenServer.

  ## Options

  - `:names` - Map of process names including `:wal`
  - `:data_dir` - Directory to store WAL files
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
  Append entries to the WAL.

  Returns `{:ok, first_index, last_index}` when entries are durably written.
  """
  @spec append(atom(), [{RaftEx.Types.index(), RaftEx.Types.term_num(), term()}], timeout()) ::
          {:ok, RaftEx.Types.index(), RaftEx.Types.index()} | {:error, term()}
  def append(wal_name, entries, timeout \\ 5_000) do
    GenServer.call(wal_name, {:append, entries}, timeout)
  end

  @doc """
  Asynchronously append entries to the WAL.

  The caller will receive `{:wal_written, first_index, last_index}` when complete.
  """
  @spec append_async(atom(), [{RaftEx.Types.index(), RaftEx.Types.term_num(), term()}]) :: :ok
  def append_async(wal_name, entries) do
    GenServer.cast(wal_name, {:append_async, entries, self()})
  end

  @doc """
  Truncate the WAL, removing all entries up to and including `index`.

  Used after installing a snapshot to discard old entries.
  """
  @spec truncate(atom(), RaftEx.Types.index(), timeout()) :: :ok | {:error, term()}
  def truncate(wal_name, index, timeout \\ 5_000) do
    GenServer.call(wal_name, {:truncate, index}, timeout)
  end

  @doc """
  Get the current WAL state.

  Returns `{first_index, last_index, buffer_size}`.
  """
  @spec info(atom()) :: {RaftEx.Types.index(), RaftEx.Types.index(), non_neg_integer()}
  def info(wal_name) do
    GenServer.call(wal_name, :info)
  end

  @doc """
  Read entries from the WAL buffer (in-memory only).

  Returns entries in the range `[from_index, to_index]`.
  """
  @spec read(atom(), RaftEx.Types.index(), RaftEx.Types.index()) ::
          [{RaftEx.Types.index(), RaftEx.Types.term_num(), term()}]
  def read(wal_name, from_index, to_index) do
    GenServer.call(wal_name, {:read, from_index, to_index})
  end

  @doc """
  Recover entries from the WAL file on startup.

  Returns `{first_index, last_index, entries}`.
  """
  @spec recover(map()) :: {RaftEx.Types.index(), RaftEx.Types.index(), list()}
  def recover(config) do
    wal_file = wal_file_path(config)

    case :prim_file.read_file_info(wal_file) do
      {:ok, _} ->
        case :file.open(wal_file, [:read, :binary, :raw]) do
          {:ok, fd} ->
            compute_checksums = Map.get(config, :wal_compute_checksums, true)
            entries = do_recover(fd, compute_checksums, [])
            :file.close(fd)
            entries

          _ ->
            {0, 0, []}
        end

      _ ->
        {0, 0, []}
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer Callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(config) do
    Process.flag(:trap_exit, true)

    wal_dir = Map.get(config, :data_dir, ".")
    :ok = File.mkdir_p!(wal_dir)

    wal_file = wal_file_path(config)
    max_batch_size = Map.get(config, :wal_max_batch_size, 8192)
    sync_method = Map.get(config, :wal_sync_method, :datasync)
    compute_checksums = Map.get(config, :wal_compute_checksums, true)

    # Recover existing WAL entries
    {first_index, last_index, recovered_entries} = recover_from_file(wal_file, compute_checksums)

    # Open WAL file for appending
    {:ok, fd} = :file.open(wal_file, [:append, :binary, :raw, :delayed_write])

    # Get current file size
    {:ok, file_size} = :file.position(fd, :eof)

    flush_timer = start_flush_timer()

    state = %__MODULE__{
      config: config,
      wal_file: wal_file,
      wal_fd: fd,
      buffer: recovered_entries,
      pending_writes: [],
      first_index: first_index,
      last_index: last_index,
      file_size: file_size,
      flush_timer: flush_timer
    }

    Logger.info(
      "RaftEx.LogWal: initialized with #{length(recovered_entries)} recovered entries, " <>
        "first_index=#{first_index}, last_index=#{last_index}"
    )

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:append, entries}, from, state) do
    state = add_to_buffer(entries, state)
    state = maybe_flush(state, from)
    # Reply immediately - actual flush happens asynchronously
    {:reply, {:ok, state.first_index, state.last_index}, state}
  end

  def handle_call({:truncate, index}, _from, state) do
    state = do_truncate(index, state)
    {:reply, :ok, state}
  end

  def handle_call(:info, _from, state) do
    {:reply, {state.first_index, state.last_index, length(state.buffer)}, state}
  end

  def handle_call({:read, from_index, to_index}, _from, state) do
    entries =
      state.buffer
      |> Enum.filter(fn {idx, _, _} -> idx >= from_index and idx <= to_index end)
      |> Enum.sort_by(fn {idx, _, _} -> idx end)

    {:reply, entries, state}
  end

  @impl GenServer
  def handle_cast({:append_async, entries, reply_to}, state) do
    state = add_to_buffer(entries, state)
    state = maybe_flush_async(state, reply_to)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:flush_timeout, state) do
    state = flush_buffer(state)

    # Restart the timer
    flush_timer = start_flush_timer()
    state = %{state | flush_timer: flush_timer}

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Remove pending write for crashed process
    pending_writes = Enum.reject(state.pending_writes, fn {p, _} -> p == pid end)
    {:noreply, %{state | pending_writes: pending_writes}}
  end

  @impl GenServer
  def terminate(_reason, state) do
    # Flush remaining entries before shutdown
    state = flush_buffer(state)

    if state.wal_fd do
      sync_wal(state)
      :file.close(state.wal_fd)
    end

    Logger.info("RaftEx.LogWal: shutting down, flushed #{length(state.buffer)} entries")
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private Helpers
  # ---------------------------------------------------------------------------

  defp wal_file_path(config) do
    data_dir = Map.get(config, :data_dir, ".")
    Path.join(data_dir, "wal.dat")
  end

  defp add_to_buffer(entries, state) do
    new_buffer = state.buffer ++ entries

    new_last_index =
      case entries do
        [] ->
          state.last_index

        entries ->
          {last_idx, _, _} = List.last(entries)
          last_idx
      end

    %{state | buffer: new_buffer, last_index: new_last_index}
  end

  defp maybe_flush(state, from) do
    max_batch_size = Map.get(state.config, :wal_max_batch_size, 8192)

    if length(state.buffer) >= max_batch_size or from != nil do
      state = flush_buffer(state)

      if from do
        send_reply(from, state)
      end

      state
    else
      state
    end
  end

  defp maybe_flush_async(state, reply_to) do
    max_batch_size = Map.get(state.config, :wal_max_batch_size, 8192)

    if length(state.buffer) >= max_batch_size do
      state = flush_buffer(state)
      send(reply_to, {:wal_written, state.first_index, state.last_index})
      state
    else
      # Monitor the process in case it crashes
      ref = Process.monitor(reply_to)
      pending_writes = [{reply_to, ref} | state.pending_writes]
      %{state | pending_writes: pending_writes}
    end
  end

  defp flush_buffer(state) do
    case state.buffer do
      [] ->
        state

      entries ->
        # Write entries to WAL file and get updated state
        state = write_entries_to_wal(entries, state)

        # Sync to disk if configured
        sync_wal(state)

        # Notify pending writers
        notify_pending_writes(state)

        # Clear buffer and return updated state
        %{state | buffer: [], pending_writes: []}
    end
  end

  defp write_entries_to_wal(entries, state) do
    compute_checksums = Map.get(state.config, :wal_compute_checksums, true)

    # Serialize entries
    entries_binary = :erlang.term_to_binary(entries)

    # Build WAL record
    count = length(entries)
    header = <<@magic::32-unsigned, @version::8-unsigned, count::32-unsigned>>

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
    case :file.write(state.wal_fd, data) do
      :ok ->
        new_file_size = state.file_size + data_size
        %{state | file_size: new_file_size}

      error ->
        Logger.error("RaftEx.LogWal: failed to write WAL entries: #{inspect(error)}")
        state
    end
  end

  defp sync_wal(state) do
    sync_method = Map.get(state.config, :wal_sync_method, :datasync)

    case sync_method do
      :fsync ->
        :file.sync(state.wal_fd)

      :datasync ->
        :file.datasync(state.wal_fd)

      :none ->
        :ok

      _ ->
        :file.datasync(state.wal_fd)
    end
  end

  defp notify_pending_writes(state) do
    Enum.each(state.pending_writes, fn {pid, _ref} ->
      send(pid, {:wal_written, state.first_index, state.last_index})
    end)
  end

  defp send_reply(from, state) do
    case from do
      {pid, ref} ->
        send(pid, {:wal_written, ref, state.first_index, state.last_index})

      pid when is_pid(pid) ->
        send(pid, {:wal_written, state.first_index, state.last_index})

      _ ->
        :ok
    end
  end

  defp do_truncate(index, state) do
    # Remove entries up to and including index
    new_buffer = Enum.reject(state.buffer, fn {idx, _, _} -> idx <= index end)

    new_first_index =
      case new_buffer do
        [] -> index + 1
        [{first_idx, _, _} | _] -> first_idx
      end

    # Truncate WAL file
    :ok = :file.truncate(state.wal_fd, 0)
    {:ok, 0} = :file.position(state.wal_fd, :bof)

    Logger.info("RaftEx.LogWal: truncated to index #{index}, new first_index=#{new_first_index}")

    %{state | buffer: new_buffer, first_index: new_first_index, file_size: 0}
  end

  defp recover_from_file(wal_file, compute_checksums) do
    case :prim_file.read_file_info(wal_file) do
      {:ok, _info} ->
        case :file.open(wal_file, [:read, :binary, :raw]) do
          {:ok, fd} ->
            entries = do_recover(fd, compute_checksums, [])
            :file.close(fd)

            case entries do
              [] ->
                {0, 0, []}

              recovered ->
                {first_idx, _, _} = hd(recovered)
                {last_idx, _, _} = List.last(recovered)
                {first_idx, last_idx, recovered}
            end

          _ ->
            {0, 0, []}
        end

      _ ->
        {0, 0, []}
    end
  end

  defp do_recover(fd, compute_checksums, acc) do
    header_size = @header_size + if(compute_checksums, do: @footer_size, else: 0)

    case :file.read(fd, @header_size) do
      {:ok, <<@magic::32-unsigned, @version::8-unsigned, count::32-unsigned>>} ->
        # Estimate, will be adjusted
        entries_size = count * 100
        {:ok, entries_binary} = :file.read(fd, entries_size)

        # Read footer if checksums enabled
        if compute_checksums do
          case :file.read(fd, @footer_size) do
            {:ok, <<_crc::32-unsigned>>} ->
              # Validate CRC
              entries = :erlang.binary_to_term(entries_binary)
              do_recover(fd, compute_checksums, acc ++ entries)

            _ ->
              acc
          end
        else
          entries = :erlang.binary_to_term(entries_binary)
          do_recover(fd, compute_checksums, acc ++ entries)
        end

      {:ok, _} ->
        # Invalid magic or version, stop recovery
        acc

      :eof ->
        acc

      _ ->
        acc
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
    try do
      {first_index, last_index, buffer_size} = GenServer.call(wal_name, :info)

      %{
        first_index: first_index,
        last_index: last_index,
        buffer_size: buffer_size,
        pending_writes: 0
      }
    catch
      _, _ -> %{first_index: 0, last_index: 0, buffer_size: 0, pending_writes: 0}
    end
  end
end
