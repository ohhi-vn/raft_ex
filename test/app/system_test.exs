defmodule RaftEx.App.SystemTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.System

  describe "default_config/0 and names" do
    test "default_config has the documented defaults" do
      config = System.default_config()

      assert config.name == :default
      assert config.wal_max_size_bytes == 256_000_000
      assert config.wal_max_batch_size == 8192
      assert config.segment_max_entries == 4096
      assert config.election_timeout_min == 500
      assert config.election_timeout_max == 2000
      assert config.command_plugs == []
      assert config.rpc_plugs == []
      assert config.names == System.default_names()
    end

    test "default_names covers all infrastructure processes" do
      names = System.default_names()

      for key <- [:wal, :wal_sup, :log_sup, :log_ets, :log_meta, :segment_writer, :directory] do
        assert is_atom(Map.fetch!(names, key))
      end
    end

    test "derive_names prefixes the system name" do
      names = System.derive_names(:my_system)

      assert names.wal == :ra_my_system_log_wal
      assert names.log_meta == :ra_my_system_log_meta
      assert names.directory == :ra_my_system_directory
    end
  end

  describe "store/1, fetch/1 and lookup_name/2" do
    test "round-trips a config through persistent term" do
      name = :"system_test_#{:erlang.unique_integer([:positive])}"
      config = %{name: name, names: %{wal: :test_wal}}
      System.store(config)

      on_exit(fn -> :persistent_term.erase({:"$ra_system", name}) end)

      assert System.fetch(name) == config
      assert System.lookup_name(name, :wal) == {:ok, :test_wal}
    end

    test "returns errors for unknown systems and keys" do
      assert System.fetch(:"missing_system_#{:erlang.unique_integer([:positive])}") == nil

      assert System.lookup_name(:"another_missing_#{:erlang.unique_integer([:positive])}", :wal) ==
               {:error, :system_not_started}
    end
  end

  describe "fetch/2" do
    test "reads a stored config from the given node" do
      name = :"system_fetch_node_#{:erlang.unique_integer([:positive])}"
      config = %{name: name, names: %{wal: :remote_wal}}
      System.store(config)

      on_exit(fn -> :persistent_term.erase({:"$ra_system", name}) end)

      assert System.fetch(name, node()) == config
    end

    test "returns nil for an unknown system on the given node" do
      name = :"system_fetch_missing_#{:erlang.unique_integer([:positive])}"
      assert System.fetch(name, node()) == nil
    end
  end
end
