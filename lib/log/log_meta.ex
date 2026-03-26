defmodule RaftEx.LogMeta do
  @moduledoc """
  Durable metadata store for Raft servers: `current_term`, `voted_for`, and
  `last_applied`.

  ## Storage layout

  Data is stored in a DETS file (`meta.dets`) and mirrored into an ETS table
  for fast reads.  Each row has the shape:

      {uid, current_term, voted_for, last_applied}

  Writes go to both DETS and ETS immediately.  Synchronous writes (`store_sync`
  and `delete_sync`) additionally call `:dets.sync/1` so the data is flushed
  to disk before the call returns.

  ## Concurrency model

  All writes are serialised through this GenServer.  Reads bypass the process
  and access ETS directly, making them safe to call from any process.
  """

  use GenServer
  require Logger

  @timeout 30_000

  # Field positions in the {uid, current_term, voted_for, last_applied} tuple.
  @pos_current_term 2
  @pos_voted_for    3
  @pos_last_applied 4

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc "Start the metadata store and link it into the supervision tree."
  def start_link(%{names: %{log_meta: name}} = cfg) do
    GenServer.start_link(__MODULE__, cfg, name: name)
  end

  @doc "Asynchronously store `{uid, key, value}`."
  @spec store(atom(), binary(), :current_term | :voted_for | :last_applied, term()) :: :ok
  def store(name, uid, key, value) when is_atom(name) do
    GenServer.cast(name, {:store, uid, key, value})
  end

  @doc "Synchronously store `{uid, key, value}`, fsyncing DETS before returning."
  @spec store_sync(atom(), binary(), :current_term | :voted_for | :last_applied, term()) :: :ok
  def store_sync(name, uid, key, value) do
    GenServer.call(name, {:store, uid, key, value}, @timeout)
  end

  @doc "Asynchronously delete all metadata for `uid`."
  @spec delete(atom(), binary()) :: :ok
  def delete(name, uid) do
    GenServer.cast(name, {:delete, uid})
  end

  @doc "Synchronously delete all metadata for `uid`, fsyncing DETS before returning."
  @spec delete_sync(atom(), binary()) :: :ok
  def delete_sync(name, uid) do
    GenServer.call(name, {:delete, uid}, @timeout)
  end

  @doc "Fetch a single metadata field, returning `nil` if absent."
  @spec fetch(atom(), binary(), :current_term | :voted_for | :last_applied) :: term() | nil
  def fetch(meta_name, uid, :current_term), do: ets_lookup(meta_name, uid, @pos_current_term)
  def fetch(meta_name, uid, :voted_for),    do: ets_lookup(meta_name, uid, @pos_voted_for)
  def fetch(meta_name, uid, :last_applied), do: ets_lookup(meta_name, uid, @pos_last_applied)

  @doc "Fetch a metadata field, returning `default` when absent."
  @spec fetch(atom(), binary(), :current_term | :voted_for | :last_applied, term()) :: term()
  def fetch(meta_name, uid, key, default) do
    case fetch(meta_name, uid, key) do
      nil -> default
      v   -> v
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(%{name: system, data_dir: dir, names: %{log_meta: tbl_name}}) do
    Process.flag(:trap_exit, true)

    :ok = File.mkdir_p!(dir)
    meta_file = Path.join(dir, "meta.dets") |> String.to_charlist()

    {:ok, ^tbl_name} = :dets.open_file(tbl_name,
      file:      meta_file,
      auto_save: 5_000
    )

    :ets.new(tbl_name, [:named_table, :public, {:read_concurrency, true}])
    ^tbl_name = :dets.to_ets(tbl_name, tbl_name)

    Logger.info(
      "ra: meta data store initialised for #{system}. " <>
      "#{:ets.info(tbl_name, :size)} record(s)"
    )

    {:ok, %{table: tbl_name}}
  end

  @impl GenServer
  def handle_call({:store, uid, key, value}, _from, %{table: tbl} = state) do
    write_meta(tbl, uid, key, value, _sync = true)
    {:reply, :ok, state}
  end

  def handle_call({:delete, uid}, _from, %{table: tbl} = state) do
    delete_uid(tbl, uid, _sync = true)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_cast({:store, uid, key, value}, %{table: tbl} = state) do
    write_meta(tbl, uid, key, value, _sync = false)
    {:noreply, state}
  end

  def handle_cast({:delete, uid}, %{table: tbl} = state) do
    delete_uid(tbl, uid, _sync = false)
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, %{table: tbl}) do
    :dets.sync(tbl)
    :dets.close(tbl)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp write_meta(tbl, uid, key, value, sync) do
    current =
      case :ets.lookup(tbl, uid) do
        []    -> {uid, nil, nil, nil}
        [row] -> row
      end

    updated = set_field(key, value, current)
    :ets.insert(tbl, updated)
    :dets.insert(tbl, updated)
    if sync, do: :dets.sync(tbl)
    :ok
  end

  defp delete_uid(tbl, uid, sync) do
    :dets.delete(tbl, uid)
    :ets.delete(tbl, uid)
    if sync, do: :dets.sync(tbl)
    :ok
  end

  # Resetting the term clears voted_for per Raft spec §5.1.
  defp set_field(:current_term, value, row) do
    if elem(row, 1) == value do
      row
    else
      row |> put_elem(@pos_voted_for - 1, nil) |> put_elem(@pos_current_term - 1, value)
    end
  end

  defp set_field(:voted_for,    value, row), do: put_elem(row, @pos_voted_for - 1, value)
  defp set_field(:last_applied, value, row), do: put_elem(row, @pos_last_applied - 1, value)

  defp ets_lookup(meta_name, uid, pos) do
    try do
      :ets.lookup_element(meta_name, uid, pos)
    rescue
      ArgumentError -> nil
    end
  end
end
