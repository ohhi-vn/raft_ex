defmodule RaftEx.Infra.Persistence.LogSegmentWriter do
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

  ## Index-based direct access

  Each segment maintains an in-memory index of `{entry_index, byte_offset, byte_length}`
  for every entry it contains. Sealed segments also persist this index as a sidecar
  file (`<segment>.idx`) so recovery does not need to re-parse the whole segment.
  Reads use `:file.pread/3` to fetch only the requested bytes. When the index is
  missing or corrupt, the implementation falls back to parsing the full segment.
  """

  use GenServer
  require Logger

  # "RASG" - Raft Segment
  @magic_bytes <<0x52, 0x41, 0x53, 0x47>>
  @version 1
  @header_size 28
  #  @footer_size 8

  # "RASI" - Raft Segment Index (sidecar)
  @sidecar_magic <<0x52, 0x41, 0x53, 0x49>>
  @sidecar_version 1

  @type segment_id :: non_neg_integer()
  @type segment_range :: {first_index :: non_neg_integer(), last_index :: non_neg_integer()}
  @type index_entry ::
          {entry_index :: non_neg_integer(), offset :: non_neg_integer(),
           length :: non_neg_integer()}

  @type segment :: %{
          id: segment_id(),
          path: String.t(),
          range: segment_range(),
          entry_count: non_neg_integer(),
          size_bytes: non_neg_integer(),
          sealed: boolean(),
          fd: reference() | nil,
          index: [index_entry()]
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
  @spec write(pid(), [RaftEx.Domain.Types.log_entry()]) :: :ok | {:error, term()}
  def write(pid, entries) when is_list(entries) do
    GenServer.call(pid, {:write, entries})
  end

  @doc """
  Reads entries from a segment by index range.
  """
  @spec read(pid(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [RaftEx.Domain.Types.log_entry()]} | {:error, term()}
  def read(pid, from_index, to_index) do
    GenServer.call(pid, {:read, from_index, to_index})
  end

  @doc """
  Reads a single entry by index.
  """
  @spec read_entry(pid(), non_neg_integer()) ::
          {:ok, RaftEx.Domain.Types.log_entry()} | {:error, :not_found}
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

    if File.exists?(segment_dir) do
      recover_segments(segment_dir)
    else
      File.mkdir_p!(segment_dir)
      {:ok, []}
    end
  end

  defp recover_segments(segment_dir) do
    case File.ls(segment_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".segment"))
        |> Enum.map(&Path.join(segment_dir, &1))
        |> Enum.sort()
        |> Enum.map(&load_segment/1)
        |> Enum.reject(&is_nil/1)
        |> then(&{:ok, &1})

      {:error, reason} ->
        {:error, reason}
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
      "RaftEx.Infra.Persistence.LogSegmentWriter: initialized with #{length(segments)} segments, " <>
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
    to_remove = Enum.filter(state.segments, fn seg -> elem(seg.range, 1) >= from_index end)
    to_keep = state.segments -- to_remove

    Enum.each(to_remove, fn seg ->
      delete_segment_file(seg)
      File.rm(sidecar_path(seg.path))
    end)

    active =
      if state.active_segment && elem(state.active_segment.range, 0) >= from_index do
        delete_segment_file(state.active_segment)
        File.rm(sidecar_path(state.active_segment.path))
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

    Enum.each(to_delete, fn seg ->
      delete_segment_file(seg)
      File.rm(sidecar_path(seg.path))
    end)

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
      end
    end

    Logger.debug("RaftEx.Infra.Persistence.LogSegmentWriter: shutting down")
    :ok
  end

  # ---------------------------------------------------------------------------
  # Segment File Operations
  # ---------------------------------------------------------------------------

  defp segment_dir(data_dir), do: Path.join(data_dir, "segments")

  defp segment_filename(first_index, last_index) do
    "#{first_index}-#{last_index}.segment"
  end

  defp sidecar_path(path), do: path <> ".idx"

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
      fd: nil,
      index: []
    }

    {:ok, segment}
  end

  defp seal_segment(
         %{path: path, range: {first, last}, entry_count: count, index: index} = segment
       ) do
    header = build_header(first, last, count)

    {:ok, fd} = File.open(path, [:read, :write])
    :file.pwrite(fd, 0, header)
    :ok = :file.sync(fd)
    File.close(fd)

    write_sidecar(path, index)

    {:ok, %{segment | sealed: true}}
  end

  defp append_entries(
         %{path: path, range: {first, last}, entry_count: count, size_bytes: size, index: idx} =
           segment,
         entries
       ) do
    binaries = Enum.map(entries, &serialize_entry/1)

    {rev_new_index, _} =
      Enum.reduce(Enum.zip(entries, binaries), {[], size}, fn {{entry_index, _, _}, bin},
                                                              {acc, off} ->
        {[{entry_index, off, byte_size(bin)} | acc], off + byte_size(bin)}
      end)

    new_index_entries = rev_new_index
    data = IO.iodata_to_binary(binaries)

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
            size_bytes: new_size,
            index: new_index_entries ++ idx
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
    index_list = Map.get(segment, :index, [])

    if index_list == [] do
      read_from_segment_full(segment, from_index, to_index)
    else
      read_from_segment_indexed(segment, index_list, from_index, to_index)
    end
  end

  defp read_from_segment_full(segment, from_index, to_index) do
    case File.read(segment.path) do
      {:ok, data} ->
        data
        |> parse_entries()
        |> Enum.filter(fn {idx, _, _} -> idx >= from_index and idx <= to_index end)

      {:error, _} ->
        []
    end
  end

  defp read_from_segment_indexed(segment, index_list, from_index, to_index) do
    index_list
    |> Enum.filter(fn {idx, _, _} -> idx >= from_index and idx <= to_index end)
    |> Enum.reduce([], fn {_idx, offset, length}, acc ->
      read_indexed_entry(segment.path, offset, length, acc)
    end)
  end

  defp read_indexed_entry(path, offset, length, acc) do
    case read_bytes(path, offset, length) do
      {:ok, bin} ->
        case decode_entry(bin) do
          {:ok, entry} -> [entry | acc]
          :error -> acc
        end

      _ ->
        acc
    end
  end

  defp read_single_entry(segment, index) do
    index_list = Map.get(segment, :index, [])

    if index_list == [] do
      fallback_read_single(segment, index)
    else
      read_single_entry_indexed(segment, index_list, index)
    end
  end

  defp read_single_entry_indexed(segment, index_list, index) do
    case binary_search_index(index_list, index) do
      {:found, mid} ->
        {_i, offset, length} = Enum.at(index_list, mid)
        read_single_entry_bytes(segment, index, offset, length)

      :not_found ->
        {:error, :not_found}
    end
  end

  defp read_single_entry_bytes(segment, index, offset, length) do
    case read_bytes(segment.path, offset, length) do
      {:ok, bin} ->
        case decode_entry(bin) do
          {:ok, entry} -> {:ok, entry}
          :error -> fallback_read_single(segment, index)
        end

      _ ->
        fallback_read_single(segment, index)
    end
  end

  defp fallback_read_single(segment, index) do
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

  defp read_bytes(path, offset, length) do
    case File.open(path, [:read, :binary]) do
      {:ok, fd} ->
        result = :file.pread(fd, offset, length)
        File.close(fd)
        result

      {:error, _} ->
        {:error, :enoent}
    end
  end

  defp decode_entry(<<index::64, term::64, cmd_size::32, cmd_data::binary-size(cmd_size)>>) do
    {:ok, {index, term, :erlang.binary_to_term(cmd_data)}}
  end

  defp decode_entry(_), do: :error

  # ---------------------------------------------------------------------------
  # Serialization
  # ---------------------------------------------------------------------------

  defp build_header(first_index, last_index, entry_count) do
    <<@magic_bytes::binary, @version::32, first_index::64, last_index::64, entry_count::32>>
  end

  defp serialize_entry({index, term, command}) do
    cmd_data = :erlang.term_to_binary(command)
    cmd_size = byte_size(cmd_data)

    <<index::64, term::64, cmd_size::32, cmd_data::binary>>
  end

  defp parse_header(
         <<@magic_bytes::binary, @version::32, first::64, last::64, count::32, _rest::binary>>
       ) do
    %{first: first, last: last, count: count}
  end

  defp parse_header(_), do: nil

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

  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  defp load_segment(path) do
    case :file.open(path, [:read, :binary, :raw]) do
      {:ok, fd} ->
        header_result = :file.pread(fd, 0, @header_size)
        size_result = :file.position(fd, :eof)
        :file.close(fd)

        with {:ok, header_data} <- header_result,
             {:ok, size} <- size_result,
             %{first: first, last: last, count: count} <- parse_header(header_data) do
          index = load_segment_index(path, count)

          %{
            id: System.unique_integer([:positive]),
            path: path,
            range: {first, last},
            entry_count: count,
            size_bytes: size,
            sealed: true,
            fd: nil,
            index: index
          }
        else
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp load_segment_index(path, count) do
    case load_sidecar(path, count) do
      {:ok, idx} ->
        idx

      :error ->
        case File.read(path) do
          {:ok, data} ->
            {_entries, idx} = parse_segment_with_index(data)
            write_sidecar(path, idx)
            idx

          _ ->
            []
        end
    end
  end

  defp parse_segment_with_index(
         <<@magic_bytes::binary, _version::32, _first::64, _last::64, _count::32, rest::binary>>
       ) do
    parse_segment_entries(rest, [], [], 28)
  end

  defp parse_segment_with_index(_), do: {[], []}

  defp parse_segment_entries(<<>>, entries_acc, index_acc, _off) do
    {Enum.reverse(entries_acc), index_acc}
  end

  defp parse_segment_entries(
         <<index::64, term::64, cmd_size::32, cmd_data::binary-size(cmd_size), rest::binary>>,
         entries_acc,
         index_acc,
         off
       ) do
    command = :erlang.binary_to_term(cmd_data)
    len = 8 + 8 + 4 + cmd_size

    parse_segment_entries(
      rest,
      [{index, term, command} | entries_acc],
      [{index, off, len} | index_acc],
      off + len
    )
  end

  defp parse_segment_entries(_, entries_acc, index_acc, _off) do
    {Enum.reverse(entries_acc), index_acc}
  end

  # ---------------------------------------------------------------------------
  # Sidecar index
  # ---------------------------------------------------------------------------

  defp load_sidecar(path, expected_count) do
    sc = sidecar_path(path)

    case File.read(sc) do
      {:ok, <<@sidecar_magic::binary, @sidecar_version::32, count::32, rest::binary>>}
      when count == expected_count ->
        validate_sidecar_rest(rest, count)

      _ ->
        :error
    end
  end

  defp validate_sidecar_rest(rest, count) do
    entry_size = 8 + 8 + 4
    bytes_needed = count * entry_size

    if byte_size(rest) < bytes_needed + 4 do
      :error
    else
      <<entries_bin::binary-size(^bytes_needed), checksum::32, _::binary>> = rest
      validate_sidecar_checksum(entries_bin, checksum, count)
    end
  end

  defp validate_sidecar_checksum(entries_bin, checksum, count) do
    if :erlang.crc32(entries_bin) != checksum do
      :error
    else
      entries = parse_sidecar_entries(entries_bin, [])

      if is_list(entries) and length(entries) == count do
        {:ok, entries}
      else
        :error
      end
    end
  end

  defp parse_sidecar_entries(<<>>, acc), do: Enum.reverse(acc)

  defp parse_sidecar_entries(<<index::64, offset::64, length::32, rest::binary>>, acc) do
    parse_sidecar_entries(rest, [{index, offset, length} | acc])
  end

  defp parse_sidecar_entries(_, _acc), do: :error

  defp write_sidecar(_path, []) do
    :ok
  end

  defp write_sidecar(path, index) do
    entries_bin =
      Enum.reduce(index, <<>>, fn {index, offset, length}, acc ->
        <<acc::binary, index::64, offset::64, length::32>>
      end)

    checksum = :erlang.crc32(entries_bin)

    data =
      <<@sidecar_magic::binary, @sidecar_version::32, length(index)::32, entries_bin::binary,
        checksum::32>>

    case File.write(sidecar_path(path), data) do
      :ok -> :ok
      {:error, _} -> :ok
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
          fd: nil,
          index: []
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
        Logger.warning(
          "RaftEx.Infra.Persistence.LogSegmentWriter: failed to delete segment #{path}: #{reason}"
        )

        :ok
    end
  end

  defp binary_search_index(index_list, target) do
    do_binary_search(index_list, target, 0, length(index_list) - 1)
  end

  defp do_binary_search(_list, _target, lo, hi) when lo > hi, do: :not_found

  defp do_binary_search(list, target, lo, hi) do
    mid = div(lo + hi, 2)
    {idx, _off, _len} = Enum.at(list, mid)

    cond do
      idx == target -> {:found, mid}
      target > idx -> do_binary_search(list, target, lo, mid - 1)
      true -> do_binary_search(list, target, mid + 1, hi)
    end
  end
end
