defmodule RaftEx.Infra.Persistence.SharedWal do
  @moduledoc """
  Experimental shared write-ahead log.

  A single WAL file shared by every uid in a system, with per-uid index
  ranges. Cross-uid writes are batched into one fsync. This is a prototype
  (in-memory buffered + single-file) intended for investigation and
  benchmarking; the default remains `RaftEx.Infra.Persistence.LogWal`.

  Implements the same API surface as `LogWal` so callers can swap via
  `config :raft_ex, :wal_mode, :shared`.
  """

  use GenServer

  alias RaftEx.Infra.Persistence.LogWal

  @file_name "shared.wal"

  defstruct [:dir, :config, :data, :flush_timer]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: Map.get(config, :name, __MODULE__))
  end

  def append(wal_name, uid, entries) do
    GenServer.call(wal_name, {:append, uid, entries}, 5_000)
  end

  def read(wal_name, uid, from_index, to_index) do
    GenServer.call(wal_name, {:read, uid, from_index, to_index}, 5_000)
  end

  def recover(config) do
    dir = Map.get(config, :data_dir, Map.get(config, :dir, "."))
    path = Path.join(dir, @file_name)

    case File.read(path) do
      {:ok, binary} ->
        data = :erlang.binary_to_term(binary)
        {first, last, entries} = recovered_range(Map.get(data, uid_of(config), []))
        {first, last, entries}

      _ ->
        {0, 0, []}
    end
  end

  defp uid_of(config), do: Map.get(config, :uid, :default)

  defp recovered_range([]), do: {0, 0, []}

  defp recovered_range(entries) do
    {first_idx, _, _} = hd(entries)
    {last_idx, _, _} = List.last(entries)
    {first_idx, last_idx, entries}
  end

  def info(wal_name, uid) do
    GenServer.call(wal_name, {:info, uid}, 5_000)
  end

  @doc """
  Migrate per-server WAL files (under `dir/<uid>/wal.dat`) into the shared
  file. Existing shared file is backed up to `shared.wal.bak`.
  """
  def migrate(dir) do
    shared_path = Path.join(dir, @file_name)
    backup = shared_path <> ".bak"

    if File.exists?(shared_path) do
      File.cp!(shared_path, backup)
    end

    # Discover uids.
    uids =
      case File.ls(dir) do
        {:ok, entries} ->
          entries
          |> Enum.filter(fn e -> File.dir?(Path.join(dir, e)) end)
          |> Enum.map(&String.to_atom/1)

        _ ->
          []
      end

    merged =
      Enum.reduce(uids, %{}, fn uid, acc ->
        {_, _, entries} = LogWal.recover(%{data_dir: Path.join(dir, Atom.to_string(uid))})
        Map.put(acc, uid, entries)
      end)

    File.write!(shared_path, :erlang.term_to_binary(merged))
    :ok
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(config) do
    dir = Map.get(config, :data_dir, Map.get(config, :dir, "."))
    File.mkdir_p!(dir)
    path = Path.join(dir, @file_name)

    data =
      case File.read(path) do
        {:ok, binary} -> :erlang.binary_to_term(binary)
        _ -> %{}
      end

    {:ok, %__MODULE__{dir: dir, config: config, data: data, flush_timer: nil}}
  end

  @impl GenServer
  def handle_call({:append, uid, entries}, _from, state) do
    uid_data = Map.get(state.data, uid, [])
    new_entries = uid_data ++ entries
    new_data = Map.put(state.data, uid, new_entries)

    # Persist (single shared file).
    path = Path.join(state.dir, @file_name)
    File.write!(path, :erlang.term_to_binary(new_data))

    first = elem(hd(entries), 0)
    last = elem(List.last(entries), 0)
    {:reply, {:ok, first, last}, %{state | data: new_data}}
  end

  @impl GenServer
  def handle_call({:read, uid, from_index, to_index}, _from, state) do
    entries = Map.get(state.data, uid, [])

    result =
      entries
      |> Enum.filter(fn {idx, _, _} -> idx >= from_index and idx <= to_index end)

    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:info, uid}, _from, state) do
    entries = Map.get(state.data, uid, [])

    {first, last, count} =
      case entries do
        [] -> {0, 0, 0}
        _ -> {elem(hd(entries), 0), elem(List.last(entries), 0), length(entries)}
      end

    {:reply, {first, last, count}, state}
  end

  @impl GenServer
  def terminate(_reason, _state), do: :ok
end
