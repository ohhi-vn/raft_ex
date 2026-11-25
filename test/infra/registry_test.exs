defmodule RaftEx.Infra.RegistryTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Registry.Directory
  alias RaftEx.Leaderboard

  setup do
    suffix = System.unique_integer([:positive])
    dir = RaftEx.TestSupport.tmp_dir("directory_test")
    directory = :"directory_reg_test_#{suffix}"
    directory_rev = :"directory_rev_reg_test_#{suffix}"

    :ets.new(directory, [:named_table, :public, {:read_concurrency, true}])

    :dets.open_file(directory_rev,
      file: String.to_charlist(Path.join(dir, "names.dets")),
      auto_save: 500,
      access: :read_write
    )

    Process.put(:registry_directory, directory)
    Process.put(:registry_directory_rev, directory_rev)

    # A system config lets us exercise every atom-based convenience function.
    system = :"registry_test_sys_#{suffix}"

    RaftEx.App.System.store(%{
      name: system,
      data_dir: dir,
      names: %{directory: directory, directory_rev: directory_rev}
    })

    Process.put(:registry_system, system)

    on_exit(fn ->
      :persistent_term.erase({:"$ra_system", system})

      try do
        :dets.close(directory_rev)
      rescue
        _ -> :ok
      end
    end)

    %{names: %{directory: directory, directory_rev: directory_rev}}
  end

  defp names,
    do: %{
      directory: Process.get(:registry_directory),
      directory_rev: Process.get(:registry_directory_rev)
    }

  test "init returns an error for an unknown system" do
    assert Directory.init(:"missing_system_#{System.unique_integer([:positive])}") ==
             {:error, :system_not_started}
  end

  test "init/1 creates the tables and restores names from the dets file" do
    suffix = System.unique_integer([:positive])
    dir = RaftEx.TestSupport.tmp_dir("directory_init_test")
    system = :"directory_init_sys_#{suffix}"
    directory = :"directory_init_tbl_#{suffix}"
    directory_rev = :"directory_init_rev_#{suffix}"
    names = %{directory: directory, directory_rev: directory_rev}

    dets_file = Path.join(dir, "names.dets") |> String.to_charlist()
    {:ok, _} = :dets.open_file(directory_rev, file: dets_file, access: :read_write)
    :ok = :dets.insert(directory_rev, {:restored_server, "restored-uid"})
    :ok = :dets.close(directory_rev)

    RaftEx.App.System.store(%{name: system, data_dir: dir, names: names})

    on_exit(fn ->
      :persistent_term.erase({:"$ra_system", system})

      try do
        :dets.close(directory_rev)
      rescue
        _ -> :ok
      end
    end)

    assert :ok = Directory.init(system)
    assert Directory.name_of(names, "restored-uid") == :restored_server
    assert Directory.uid_of(names, :restored_server) == "restored-uid"
    assert Directory.where_is(names, "restored-uid") == nil
  end

  test "register, query and unregister a server" do
    n = names()
    pid = self()
    parent = self()
    assert :ok = Directory.register_name(n, "uid-1", pid, parent, :server1, :cluster1)

    assert Directory.where_is(n, :server1) == pid
    assert Directory.where_is(n, "uid-1") == pid
    assert Directory.where_is_parent(n, :server1) == parent
    assert Directory.where_is_parent(n, "uid-1") == parent
    assert Directory.name_of(n, "uid-1") == :server1
    assert Directory.cluster_name_of(n, "uid-1") == :cluster1
    assert Directory.pid_of(n, "uid-1") == pid
    assert Directory.uid_of(n, :server1) == "uid-1"
    assert Directory.uid_of(n, {:server1, node()}) == "uid-1"
    assert Directory.is_registered_uid(n, "uid-1")
    refute Directory.is_registered_uid(n, "unknown")
    assert Directory.list_registered(n) == [server1: "uid-1"]

    assert Directory.unregister_name(n, "uid-1") in [:ok, "uid-1"]
    refute Directory.is_registered_uid(n, "uid-1")
    assert Directory.where_is(n, :server1) == nil
  end

  test "querying unknown entries" do
    n = names()
    assert Directory.where_is(n, :unknown) == nil
    assert Directory.name_of(n, "unknown") == nil
    assert Directory.cluster_name_of(n, "unknown") == nil
    assert Directory.pid_of(n, "unknown") == nil
    assert Directory.uid_of(n, :unknown) == nil
    assert Directory.where_is_parent(n, :unknown) == nil
  end

  test "registering a different uid for a known server replaces the dets mapping" do
    n = names()
    pid = self()

    assert :ok = Directory.register_name(n, "old-uid", pid, pid, :replaced_server, :cluster1)
    assert :ok = Directory.register_name(n, "new-uid", pid, pid, :replaced_server, :cluster1)

    assert Directory.uid_of(n, :replaced_server) == "new-uid"
    # The old ETS entry is left in place; only the reverse mapping is replaced.
    assert Directory.name_of(n, "old-uid") == :replaced_server
  end

  test "unregistering an unknown uid takes the select_delete branch" do
    n = names()
    :ok = :dets.insert(n.directory_rev, {:orphan_server, "orphan-uid"})

    assert Directory.unregister_name(n, "orphan-uid") == "orphan-uid"
  end

  test "lookups return nil when stored values are not pids" do
    n = names()

    :ets.insert(n.directory, {"bad-pid-uid", :not_a_pid, self(), :bad_pid_server, nil})
    :ets.insert(n.directory, {"bad-parent-uid", self(), :not_a_pid, :bad_parent_server, nil})

    assert Directory.where_is(n, "bad-pid-uid") == nil
    assert Directory.where_is_parent(n, "bad-parent-uid") == nil
  end

  test "all atom-based system helpers resolve names from the system config" do
    system = Process.get(:registry_system)
    pid = self()

    assert :ok =
             Directory.register_name(system, "atom-uid", pid, pid, :atom_server, :atom_cluster)

    assert Directory.where_is(system, :atom_server) == pid
    assert Directory.where_is_parent(system, :atom_server) == pid
    assert Directory.uid_of(system, :atom_server) == "atom-uid"
    assert Directory.name_of(system, "atom-uid") == :atom_server
    assert Directory.pid_of(system, "atom-uid") == pid
    assert Directory.cluster_name_of(system, "atom-uid") == :atom_cluster
    assert Directory.is_registered_uid(system, "atom-uid")
    assert Directory.list_registered(system) == [atom_server: "atom-uid"]

    assert Directory.unregister_name(system, "atom-uid") == "atom-uid"
    refute Directory.is_registered_uid(system, "atom-uid")
  end

  test "overview merges ra state and snapshot state" do
    n = names()
    system = Process.get(:registry_system)
    uid = "overview-uid"
    server = :overview_server

    assert :ok = Directory.register_name(n, uid, self(), self(), server, :overview_cluster)
    :ets.insert(:ra_state, {server, :leader, [:m1, :m2]})
    :ets.insert(:ra_log_snapshot_state, {uid, 11, 7, [8, 9]})

    on_exit(fn ->
      :ets.delete(:ra_state, server)
      :ets.delete(:ra_log_snapshot_state, uid)
    end)

    assert %{^server => row} = Directory.overview(system)
    assert row.uid == uid
    assert row.pid == self()
    assert row.cluster_name == :overview_cluster
    assert row.state == :leader
    assert row.membership == [:m1, :m2]
    assert row.snapshot_state == {11, 7, [8, 9]}
  end

  test "Leaderboard record, lookup and clear" do
    Leaderboard.init()
    cluster = :"leaderboard_test_#{System.unique_integer([:positive])}"
    leader = {:server1, node()}
    members = [leader, {:server2, node()}]

    assert :ok = Leaderboard.record(cluster, leader, members)
    assert Leaderboard.lookup_leader(cluster) == leader
    assert Leaderboard.lookup_members(cluster) == members
    assert %DateTime{} = Leaderboard.lookup_timestamp(cluster)
    assert is_list(Leaderboard.overview())
    assert Leaderboard.local_server_id(cluster) == leader

    assert :ok = Leaderboard.clear(cluster)
    assert Leaderboard.lookup_leader(cluster) == nil
  end

  test "Leaderboard tolerates a missing table and missing entries" do
    Leaderboard.init()
    table = :"Elixir.RaftEx.Leaderboard"
    owner = Process.whereis(RaftEx.Infra.Observability.MetricsEts)

    restore = fn ->
      if :ets.info(table) == :undefined do
        tid = :ets.new(table, [:set, :named_table, :public])
        :ets.give_away(tid, owner, :ok)
      end
    end

    on_exit(restore)
    :ets.delete(table)

    assert Leaderboard.lookup_leader(:missing) == nil
    assert Leaderboard.lookup_members(:missing) == nil
    assert Leaderboard.lookup_timestamp(:missing) == nil
    assert Leaderboard.forget_leader({:never, node()}) == :ok

    restore.()
    assert Leaderboard.init() == :ok
  end

  test "Leaderboard forget_leader clears the leader but keeps members" do
    Leaderboard.init()
    cluster = :"leaderboard_forget_#{System.unique_integer([:positive])}"
    leader = {:server1, node()}
    members = [leader, {:server2, node()}]

    assert :ok = Leaderboard.record(cluster, leader, members)
    assert :ok = Leaderboard.forget_leader(leader)

    assert Leaderboard.lookup_leader(cluster) == nil
    assert Leaderboard.lookup_members(cluster) == members
    assert %DateTime{} = Leaderboard.lookup_timestamp(cluster)
  end
end
