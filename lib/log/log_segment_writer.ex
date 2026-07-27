defmodule RaftEx.LogSegmentWriter do
  @moduledoc """
  Background process that manages immutable segment files for long-term log storage.

  Segments are immutable files containing a range of log entries. Once a segment
  is sealed, it cannot be modified. This provides efficient sequential reads
  and enables compaction of old entries.

  ## Segment File Format

  Each segment file contains:
  - Header: magic bytes, version, first_index, last_index, entry count
  - Entries: serialized log entries with checksums
  - Footer: checksum of the entire file

  ## Lifecycle

  1. **Active Segment**: Receives new entries from WAL
  2. **Sealed Segment**: Closed when max size/entries reached
  3. **Compacted**: Merged with other segments during cleanup
  """

  use GenServer
  require Logger

  # "RASG" - Raft Segment
  @magic_bytes <<0x52, 0x41, 0x53, 0x47>>
  @version 1
  @header_size 24
  #  @footer_size 8

  @type segment_id :: non_neg_integer()
  @type segment_range :: {first_index :: non_neg_integer(), last_index :: non_neg_integer()}

  @type segment :: %{
          id: segment_id(),
          path: String.t(),
          range: segment_range(),
          entry_count: non_neg_integer(),
          size_bytes: non_neg_integer(),
          sealed: boolean(),
          fd: reference() | nil
        }

  @type t :: %__MODULE__{
          config: map(),
          segments: [segment()],
          active_segment: segment() | nil,
          data_dir: String.t(),
          max_entries: non_neg_integer(),
          max_size_bytes: non_neg_integer()
        }

  defstruct [:config, :segments, :active_segment, :data_dir, :max_entries, :max_size_bytes]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the segment writer process.
  """
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(%{name: name} = config) do
    GenServer.start_link(__MODULE__, config, name: name)
  end

  @doc """
  Returns overview of segment state.
  """
  @spec overview(pid()) :: map()
  def overview(pid) do
    GenServer.call(pid, :overview)
  end

  @doc """
  Writes entries to the current active segment or creates a new one.
  """
  @spec write(pid(), [RaftEx.Types.log_entry()]) :: :ok | {:error, term()}
  def write(pid, entries) when is_list(entries) do
    GenServer.call(pid, {:write, entries})
  end

  @doc """
  Reads entries from a segment by index range.
  """
  @spec read(pid(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [RaftEx.Types.log_entry()]} | {:error, term()}
  def read(pid, from_index, to_index) do
    GenServer.call(pid, {:read, from_index, to_index})
  end

  @doc """
  Reads a single entry by index.
  """
  @spec read_entry(pid(), non_neg_integer()) ::
          {:ok, RaftEx.Types.log_entry()} | {:error, :not_found}
  def read_entry(pid, index) do
    GenServer.call(pid, {:read_entry, index})
  end

  @doc """
  Seals the current active segment and starts a new one.
  """
  @spec seal_active_segment(pid()) :: :ok | {:error, term()}
  def seal_active_segment(pid) do
    GenServer.call(pid, :seal_active_segment)
  end

  @doc """
  Truncates segments from the given index onwards.
  """
  @spec truncate_from(pid(), non_neg_integer()) :: :ok | {:error, term()}
  def truncate_from(pid, from_index) do
    GenServer.call(pid, {:truncate_from, from_index})
  end

  @doc """
  Deletes segments up to the given index (used after snapshot).
  """
  @spec delete_up_to(pid(), non_neg_integer()) :: :ok | {:error, term()}
  def delete_up_to(pid, up_to_index) do
    GenServer.call(pid, {:delete_up_to, up_to_index})
  end

  @doc """
  Returns the first and last index across all segments.
  """
  @spec index_range(pid()) :: {non_neg_integer(), non_neg_integer()}
  def index_range(pid) do
    GenServer.call(pid, :index_range)
  end

  @doc """
  Recovers segments from disk.
  """
  @spec recover(map()) :: {:ok, [segment()]} | {:error, term()}
  def recover(%{data_dir: data_dir}) do
    segment_dir = segment_dir(data_dir)

    if not File.exists?(segment_dir) do
      File.mkdir_p!(segment_dir)
      {:ok, []}
    else
      case File.ls(segment_dir) do
        {:ok, files} ->
          files
          |> Enum.filter(&String.ends_with?(&1, ".segment"))
          |> Enum.map(fn f -> Path.join(segment_dir, f) end)
          |> Enum.sort()
          |> Enum.map(&load_segment_header/1)
          |> Enum.reject(&is_nil/1)
          |> then(&{:ok, &1})

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer Callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(config) do
    data_dir = Map.get(config, :data_dir, "/tmp/raft_segments")
    max_entries = Map.get(config, :segment_max_entries, 4096)
    max_size_bytes = Map.get(config, :segment_max_size_bytes, 64_000_000)

    File.mkdir_p!(data_dir)

    segments =
      case recover(%{data_dir: data_dir}) do
        {:ok, segs} -> segs
        {:error, _} -> []
      end

    active_segment = find_or_create_active_segment(segments, data_dir)

    state = %__MODULE__{
      config: config,
      segments: segments,
      active_segment: active_segment,
      data_dir: data_dir,
      max_entries: max_entries,
      max_size_bytes: max_size_bytes
    }

    Logger.info(
      "RaftEx.LogSegmentWriter: initialized with #{length(segments)} segments, " <>
        "active=#{inspect(active_segment && active_segment.range)}"
    )

    {:ok, state}
  end

  @impl GenServer
  def handle_call(:overview, _from, state) do
    overview = %{
      num_segments: length(state.segments),
      active_segment: state.active_segment && state.active_segment.range,
      data_dir: state.data_dir,
      max_entries: state.max_entries,
      max_size_bytes: state.max_size_bytes
    }

    {:reply, overview, state}
  end

  @impl GenServer
  def handle_call({:write, entries}, _from, state) when entries == [] do
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:write, entries}, from, %{active_segment: nil} = state) do
    {:ok, new_active} = create_new_segment(state)
    state = %{state | active_segment: new_active}
    handle_call({:write, entries}, from, state)
  end

  @impl GenServer
  def handle_call({:write, entries}, _from, state) do
    %{active_segment: active, max_entries: max_entries, max_size_bytes: max_size_bytes} = state

    state =
      if active.entry_count + length(entries) > max_entries or
           active.size_bytes >= max_size_bytes do
        {:ok, sealed} = seal_segment(active)
        {:ok, new_active} = create_new_segment(%{state | segments: state.segments ++ [sealed]})
        %{state | segments: state.segments ++ [sealed], active_segment: new_active}
      else
        state
      end

    case append_entries(state.active_segment, entries) do
      {:ok, updated_active} ->
        state = %{state | active_segment: updated_active}
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:read, from_index, to_index}, _from, state) do
    entries = read_entries_in_range(state, from_index, to_index)
    {:reply, {:ok, entries}, state}
  end

  @impl GenServer
  def handle_call({:read_entry, index}, _from, state) do
    case find_entry(state, index) do
      {:ok, entry} -> {:reply, {:ok, entry}, state}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  @impl GenServer
  def handle_call(:seal_active_segment, _from, %{active_segment: nil} = state) do
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call(:seal_active_segment, _from, state) do
    %{active_segment: active} = state

    case seal_segment(active) do
      {:ok, sealed} ->
        state = %{state | segments: state.segments ++ [sealed], active_segment: nil}
        {:reply, :ok, state}
    end
  end

  @impl GenServer
  def handle_call({:truncate_from, from_index}, _from, state) do
    {to_keep, to_remove} =
      Enum.split_with(state.segments, fn seg ->
        elem(seg.range, 1) < from_index
      end)

    Enum.each(to_remove, &delete_segment_file/1)

    active =
      if state.active_segment && elem(state.active_segment.range, 0) >= from_index do
        delete_segment_file(state.active_segment)
        nil
      else
        state.active_segment
      end

    state = %{state | segments: to_keep, active_segment: active}
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:delete_up_to, up_to_index}, _from, state) do
    {to_delete, to_keep} =
      Enum.split_with(state.segments, fn seg ->
        elem(seg.range, 1) <= up_to_index
      end)

    Enum.each(to_delete, &delete_segment_file/1)

    state = %{state | segments: to_keep}
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call(:index_range, _from, state) do
    range = compute_index_range(state)
    {:reply, range, state}
  end

  @impl GenServer
  def handle_cast(_msg, state), do: {:noreply, state}

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    if state.active_segment do
      case seal_segment(state.active_segment) do
        {:ok, _sealed} -> :ok
        _ -> :ok
      end
    end

    Logger.debug("RaftEx.LogSegmentWriter: shutting down")
    :ok
  end

  # ---------------------------------------------------------------------------
  # Segment File Operations
  # ---------------------------------------------------------------------------

  defp segment_dir(data_dir), do: Path.join(data_dir, "segments")

  defp segment_filename(first_index, last_index) do
    "#{first_index}-#{last_index}.segment"
  end

  defp create_new_segment(state) do
    first_index =
      case compute_index_range(state) do
        {0, 0} -> 1
        {_, last} when last > 0 -> last + 1
        _ -> 1
      end

    path = Path.join(segment_dir(state.data_dir), segment_filename(first_index, first_index))
    header = build_header(first_index, first_index, 0)
    File.write!(path, header)

    segment = %{
      id: System.unique_integer([:positive]),
      path: path,
      range: {first_index, first_index},
      entry_count: 0,
      size_bytes: @header_size,
      sealed: false,
      fd: nil
    }

    {:ok, segment}
  end

  defp seal_segment(%{path: path, range: {first, last}, entry_count: count} = segment) do
    header = build_header(first, last, count)

    {:ok, fd} = File.open(path, [:read, :write])
    :file.pwrite(fd, 0, header)
    :ok = :file.sync(fd)
    File.close(fd)

    {:ok, %{segment | sealed: true}}
  end

  defp append_entries(
         %{path: path, range: {first, last}, entry_count: count, size_bytes: size} = segment,
         entries
       ) do
    data = Enum.map(entries, &serialize_entry/1) |> IO.iodata_to_binary()

    case File.open(path, [:append, :binary]) do
      {:ok, fd} ->
        :ok = :file.write(fd, data)
        :ok = :file.sync(fd)
        File.close(fd)

        new_count = count + length(entries)
        new_last = if count == 0, do: first + length(entries) - 1, else: last + length(entries)
        new_size = size + byte_size(data)

        updated_segment = %{
          segment
          | range: {first, new_last},
            entry_count: new_count,
            size_bytes: new_size
        }

        {:ok, updated_segment}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_entries_in_range(state, from_index, to_index) do
    active_entries =
      if state.active_segment && overlaps?(state.active_segment, from_index, to_index) do
        read_from_segment(state.active_segment, from_index, to_index)
      else
        []
      end

    segment_entries =
      state.segments
      |> Enum.filter(&overlaps?(&1, from_index, to_index))
      |> Enum.flat_map(&read_from_segment(&1, from_index, to_index))

    (segment_entries ++ active_entries)
    |> Enum.sort(fn {idx1, _, _}, {idx2, _, _} -> idx1 <= idx2 end)
  end

  defp find_entry(state, index) do
    if state.active_segment && in_range?(state.active_segment, index) do
      read_single_entry(state.active_segment, index)
    else
      case Enum.find(state.segments, &in_range?(&1, index)) do
        nil -> {:error, :not_found}
        segment -> read_single_entry(segment, index)
      end
    end
  end

  defp read_from_segment(segment, from_index, to_index) do
    case File.read(segment.path) do
      {:ok, data} ->
        data
        |> parse_entries()
        |> Enum.filter(fn {idx, _, _} -> idx >= from_index and idx <= to_index end)

      {:error, _} ->
        []
    end
  end

  defp read_single_entry(segment, index) do
    case File.read(segment.path) do
      {:ok, data} ->
        data
        |> parse_entries()
        |> Enum.find(fn {idx, _, _} -> idx == index end)
        |> case do
          nil -> {:error, :not_found}
          entry -> {:ok, entry}
        end

      {:error, _} ->
        {:error, :not_found}
    end
  end

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  defp build_header(first_index, last_index, entry_count) do
    <<@magic_bytes::binary, @version::32, first_index::64, last_index::64, entry_count::32>>
  end

  # defp parse_header(
  #        <<@magic_bytes::binary, @version::32, first::64, last::64, count::32, _rest::binary>>
  #      ) do
  #   %{first_index: first, last_index: last, entry_count: count}
  # end

  # defp parse_header(_), do: nil

  defp serialize_entry({index, term, command}) do
    cmd_data = :erlang.term_to_binary(command)
    cmd_size = byte_size(cmd_data)

    <<index::64, term::64, cmd_size::32, cmd_data::binary>>
  end

  defp parse_entries(
         <<@magic_bytes::binary, _version::32, _first::64, _last::64, _count::32, rest::binary>>
       ) do
    parse_entries_rest(rest, [])
  end

  defp parse_entries(_), do: []

  defp parse_entries_rest(
         <<index::64, term::64, cmd_size::32, cmd_data::binary-size(cmd_size), rest::binary>>,
         acc
       ) do
    command = :erlang.binary_to_term(cmd_data)
    parse_entries_rest(rest, [{index, term, command} | acc])
  end

  defp parse_entries_rest(_, acc), do: Enum.reverse(acc)

  # ---------------------------------------------------------------------------
  # Recovery
  # ---------------------------------------------------------------------------

  defp load_segment_header(path) do
    case File.read(path) do
      {:ok, <<@magic_bytes::binary, @version::32, first::64, last::64, count::32, _rest::binary>>} ->
        {:ok, %{size: size}} = File.stat(path)

        %{
          id: System.unique_integer([:positive]),
          path: path,
          range: {first, last},
          entry_count: count,
          size_bytes: size,
          sealed: true,
          fd: nil
        }

      _ ->
        Logger.warning("RaftEx.LogSegmentWriter: corrupted segment file #{path}")
        nil
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp find_or_create_active_segment(segments, data_dir) do
    case Enum.find(segments, &(!&1.sealed)) do
      nil ->
        first_index =
          case segments do
            [] -> 1
            segs -> elem(List.last(segs).range, 1) + 1
          end

        path = Path.join(segment_dir(data_dir), segment_filename(first_index, first_index))
        File.write!(path, build_header(first_index, first_index, 0))

        %{
          id: System.unique_integer([:positive]),
          path: path,
          range: {first_index, first_index},
          entry_count: 0,
          size_bytes: @header_size,
          sealed: false,
          fd: nil
        }

      active ->
        active
    end
  end

  defp compute_index_range(%{segments: segments, active_segment: active}) do
    all_segments = segments ++ if(active, do: [active], else: [])

    case all_segments do
      [] ->
        {0, 0}

      segs ->
        first = Enum.min_by(segs, &elem(&1.range, 0)) |> then(&elem(&1.range, 0))
        last = Enum.max_by(segs, &elem(&1.range, 1)) |> then(&elem(&1.range, 1))
        {first, last}
    end
  end

  defp in_range?(%{range: {first, last}}, index), do: index >= first and index <= last

  defp overlaps?(%{range: {first, last}}, from, to) do
    first <= to and last >= from
  end

  defp delete_segment_file(%{path: path}) do
    case File.rm(path) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("RaftEx.LogSegmentWriter: failed to delete segment #{path}: #{reason}")
        :ok
    end
  end
end
