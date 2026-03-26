defmodule RaftEx.Directory do
  require Logger

  def init(system) when is_atom(system) do
    case RaftEx.System.fetch(system) do
      nil -> {:error, :system_not_started}
      %{data_dir: dir, names: names} -> init(dir, names)
    end
  end

  def init(dir, %{directory: name, directory_rev: name_rev}) do
    :ets.new(name, [:named_table, :public, {:read_concurrency, true}])
    RaftEx.Lib.make_dir(dir)

    dets_file = Path.join(dir, "names.dets") |> String.to_charlist()

    {:ok, ^name_rev} =
      :dets.open_file(name_rev, file: dets_file, auto_save: 500, access: :read_write)

    :dets.foldl(
      fn {server_name, uid}, _acc ->
        :ets.insert(name, {uid, nil, nil, server_name, nil})
      end,
      :ok,
      name_rev
    )

    :ok
  end

  def register_name(system, uid, pid, parent_pid, server_name, cluster_name)
      when is_atom(system) do
    names = get_names(system)
    register_name(names, uid, pid, parent_pid, server_name, cluster_name)
  end

  def register_name(
        %{directory: directory, directory_rev: dir_rev} = system,
        uid,
        pid,
        parent_pid,
        server_name,
        cluster_name
      ) do
    true = :ets.insert(directory, {uid, pid, parent_pid, server_name, cluster_name})

    case uid_of(system, server_name) do
      nil ->
        :ok = :dets.insert(dir_rev, {server_name, uid})

      ^uid ->
        :ok

      other_uid ->
        :ok = :dets.insert(dir_rev, {server_name, uid})
        Logger.warning("ra: server #{server_name} uid #{uid} replaces prior #{other_uid}")
    end
  end

  def unregister_name(system, uid) when is_atom(system) do
    unregister_name(get_names(system), uid)
  end

  def unregister_name(%{directory: directory, directory_rev: dir_rev}, uid) do
    case :ets.take(directory, uid) do
      [{_, _, _, server_name, _}] ->
        :ok = :dets.delete(dir_rev, server_name)
        uid

      [] ->
        :dets.select_delete(dir_rev, [{{{:_, uid}}, [], [true]}])
        uid
    end
  end

  def where_is(system, server_name) when is_atom(system) do
    where_is(get_names(system), server_name)
  end

  def where_is(%{directory_rev: dir_rev} = names, server_name) when is_atom(server_name) do
    case :dets.lookup(dir_rev, server_name) do
      [] -> nil
      [{_, uid}] -> where_is(names, uid)
    end
  end

  def where_is(%{directory: dir}, uid) when is_binary(uid) do
    case :ets.lookup(dir, uid) do
      [{_, pid, _, _, _}] when is_pid(pid) -> pid
      _ -> nil
    end
  end

  def where_is_parent(system, server_name) when is_atom(system) do
    where_is_parent(get_names(system), server_name)
  end

  def where_is_parent(%{directory_rev: dir_rev} = names, server_name) when is_atom(server_name) do
    case :dets.lookup(dir_rev, server_name) do
      [] -> nil
      [{_, uid}] -> where_is_parent(names, uid)
    end
  end

  def where_is_parent(%{directory: dir}, uid) when is_binary(uid) do
    case :ets.lookup(dir, uid) do
      [{_, _, pid, _, _}] when is_pid(pid) -> pid
      _ -> nil
    end
  end

  def name_of(system_or_names, uid) do
    tbl = get_tbl(system_or_names)

    case :ets.lookup(tbl, uid) do
      [{_, _, _, server_name, _}] -> server_name
      [] -> nil
    end
  end

  def cluster_name_of(system_or_names, uid) do
    tbl = get_tbl(system_or_names)

    case :ets.lookup(tbl, uid) do
      [{_, _, _, _, cluster_name}] when cluster_name != nil -> cluster_name
      _ -> nil
    end
  end

  def pid_of(system_or_names, uid) do
    case :ets.lookup(get_tbl(system_or_names), uid) do
      [{_, pid, _, _, _}] when is_pid(pid) -> pid
      _ -> nil
    end
  end

  def uid_of(system, server_name) when is_atom(system) do
    uid_of(get_names(system), server_name)
  end

  def uid_of(%{directory_rev: tbl}, server_name) when is_atom(server_name) do
    case :dets.lookup(tbl, server_name) do
      [] -> nil
      [{_, uid}] -> uid
    end
  end

  def uid_of(system_or_names, {server_name, _}) when is_atom(server_name) do
    uid_of(system_or_names, server_name)
  end

  def is_registered_uid(system_or_names, uid) when is_binary(uid) do
    name_of(system_or_names, uid) != nil
  end

  def list_registered(system_or_names) do
    tbl = get_reverse(system_or_names)
    :dets.select(tbl, [{:_, [], [:"$_"]}])
  end

  def overview(system) when is_atom(system) do
    %{directory: tbl} = get_names(system)
    dir = :ets.tab2list(tbl)

    rows =
      :ets.tab2list(:ra_state)
      |> Enum.map(fn {k, s, v} -> {k, {s, v}} end)
      |> Map.new()

    snaps =
      :ets.tab2list(:ra_log_snapshot_state)
      |> Enum.reduce(%{}, fn t, acc ->
        Map.put(acc, elem(t, 0), Tuple.delete_at(t, 0))
      end)

    Enum.reduce(dir, %{}, fn {uid, pid, parent, server_name, cluster_name}, acc ->
      {s, v} = Map.get(rows, server_name, {nil, nil})

      Map.put(acc, server_name, %{
        uid: uid,
        pid: pid,
        parent: parent,
        state: s,
        membership: v,
        cluster_name: cluster_name,
        snapshot_state: Map.get(snaps, uid)
      })
    end)
  end

  # ---- private -------------------------------------------------------

  defp get_tbl(%{directory: tbl}), do: tbl

  defp get_tbl(system) when is_atom(system) do
    {:ok, tbl} = RaftEx.System.lookup_name(system, :directory)
    tbl
  end

  defp get_reverse(system) when is_atom(system) do
    {:ok, tbl} = RaftEx.System.lookup_name(system, :directory_rev)
    tbl
  end

  defp get_reverse(%{directory_rev: dir_rev}), do: dir_rev

  defp get_names(system) when is_atom(system) do
    %{names: names} = RaftEx.System.fetch(system)
    names
  end
end
