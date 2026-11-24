defmodule RaftEx.Infra.ConfigEnvFilesystemTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Config.Env
  alias RaftEx.Infra.Filesystem

  describe "Env" do
    test "data_dir includes the node name" do
      assert Env.data_dir() |> Path.basename() == node() |> Atom.to_string()
    end

    test "server_data_dir uses the stored system config" do
      system = :"env_test_#{System.unique_integer([:positive])}"
      RaftEx.App.System.store(%{name: system, data_dir: "/tmp/raft_env_test"})
      on_exit(fn -> :persistent_term.erase({:"$ra_system", system}) end)

      assert Env.server_data_dir(system, "uid-1") == "/tmp/raft_env_test/uid-1"
      assert Env.server_data_dir(system, :uid2) == "/tmp/raft_env_test/uid2"
    end

    test "configure_logger stores the module" do
      Env.configure_logger(RaftEx.Infra.Observability.Monitor)
      assert :persistent_term.get(:"$ra_logger") == RaftEx.Infra.Observability.Monitor
    end
  end

  describe "Filesystem" do
    test "make_dir, is_dir and recursive_delete" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_test")
      nested = Path.join(dir, "a/b/c")

      assert :ok = Filesystem.make_dir(nested)
      assert Filesystem.is_dir(nested)
      refute Filesystem.is_dir(Path.join(dir, "missing"))

      assert :ok = Filesystem.recursive_delete(nested)
      refute Filesystem.is_dir(nested)
    end

    test "write_file, is_file, is_any_file and ensure_dir" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_write_test")
      path = Path.join(dir, "sub/file.txt")

      assert :ok = Filesystem.ensure_dir(path)
      assert :ok = Filesystem.write_file(path, ["hello", " world"], false)
      assert Filesystem.is_file(path)
      assert Filesystem.is_any_file(path)
      refute Filesystem.is_file(dir)
      refute Filesystem.is_any_file(Path.join(dir, "missing"))
      assert :ok = Filesystem.ensure_dir(path)
    end

    test "write_file with sync" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_sync_test")
      assert :ok = Filesystem.write_file(Path.join(dir, "synced.txt"), "data")
    end

    test "error paths" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_error_test")
      missing = Path.join(dir, "missing")

      assert {:error, _} = Filesystem.rename(missing, Path.join(dir, "dst"))
      assert :ok = Filesystem.recursive_delete(missing)
    end

    test "sync, sync_file and rename" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_rename_test")
      src = Path.join(dir, "src.txt")
      dst = Path.join(dir, "dst.txt")
      Filesystem.write_file(src, "data", false)

      {:ok, fd} = File.open(src, [:read])
      assert :ok = Filesystem.sync(fd)
      File.close(fd)
      assert :ok = Filesystem.sync_file(src)
      assert :ok = Filesystem.rename(src, dst)
      assert Filesystem.is_file(dst)
    end
  end
end
