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

    on_exit(fn ->
      :dets.close(directory_rev)
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
end
