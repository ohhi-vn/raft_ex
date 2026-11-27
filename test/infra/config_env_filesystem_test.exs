defmodule RaftEx.Infra.ConfigEnvFilesystemTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Config.Env
  alias RaftEx.Infra.Filesystem

  describe "Env" do
    test "data_dir includes the node name" do
      assert Env.data_dir() |> Path.basename() == node() |> Atom.to_string()
    end

    test "data_dir ignores the legacy ra key" do
      original_raft_ex = Application.get_env(:raft_ex, :data_dir)
      original_ra = Application.get_env(:ra, :data_dir)

      on_exit(fn ->
        case original_raft_ex do
          nil -> Application.delete_env(:raft_ex, :data_dir)
          val -> Application.put_env(:raft_ex, :data_dir, val)
        end

        case original_ra do
          nil -> Application.delete_env(:ra, :data_dir)
          val -> Application.put_env(:ra, :data_dir, val)
        end
      end)

      Application.put_env(:ra, :data_dir, "/tmp/raft_legacy")
      Application.put_env(:raft_ex, :data_dir, "/tmp/raft_ex_current")
      assert Path.dirname(Env.data_dir()) == "/tmp/raft_ex_current"

      Application.delete_env(:raft_ex, :data_dir)
      # Legacy `:ra` values are ignored; falls back to cwd.
      {:ok, cwd} = File.cwd()
      assert Env.data_dir() == Path.join(cwd, Atom.to_string(node()))
    end

    test "server_data_dir uses the stored system config" do
      system = :"env_test_#{System.unique_integer([:positive])}"
      RaftEx.App.System.store(%{name: system, data_dir: "/tmp/raft_env_test"})
      on_exit(fn -> :persistent_term.erase({:"$ra_system", system}) end)

      assert Env.server_data_dir(system, "uid-1") == "/tmp/raft_env_test/uid-1"
      assert Env.server_data_dir(system, :uid2) == "/tmp/raft_env_test/uid2"
    end

    test "configure_logger is a no-op (legacy hook removed)" do
      assert :ok = Env.configure_logger(RaftEx.Infra.Observability.Monitor)
    end
  end

  describe "Filesystem" do
    test "make_dir, dir? and recursive_delete" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_test")
      nested = Path.join(dir, "a/b/c")

      assert :ok = Filesystem.make_dir(nested)
      assert Filesystem.dir?(nested)
      refute Filesystem.dir?(Path.join(dir, "missing"))

      assert :ok = Filesystem.recursive_delete(nested)
      refute Filesystem.dir?(nested)
    end

    test "write_file, file?, any_file? and ensure_dir" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_write_test")
      path = Path.join(dir, "sub/file.txt")

      assert :ok = Filesystem.ensure_dir(path)
      assert :ok = Filesystem.write_file(path, ["hello", " world"], false)
      assert Filesystem.file?(path)
      assert Filesystem.any_file?(path)
      refute Filesystem.file?(dir)
      refute Filesystem.any_file?(Path.join(dir, "missing"))
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
      assert Filesystem.file?(dst)
    end

    test "sync_file reports failures to open the file" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_sync_error_test")
      missing = Path.join(dir, "no_such_dir") |> Path.join("f")

      assert {:error, :enoent} = Filesystem.sync_file(missing)
    end

    test "recursive_delete throws when the directory cannot be removed" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_delete_error_test")
      locked = Path.join(dir, "locked")
      child = Path.join(locked, "child")
      :ok = Filesystem.make_dir(child)
      File.write!(Path.join(child, "f"), "data")
      File.chmod!(locked, 0o500)
      on_exit(fn -> File.chmod!(locked, 0o755) end)

      assert catch_throw(Filesystem.recursive_delete(child)) == {:error, :eacces}

      File.chmod!(locked, 0o755)
    end

    test "rename retries transient permission errors" do
      dir = RaftEx.TestSupport.tmp_dir("filesystem_retry_test")
      src = Path.join(dir, "src.txt")
      File.write!(src, "data")
      locked = Path.join(dir, "locked")
      :ok = Filesystem.make_dir(locked)
      File.chmod!(locked, 0o000)
      on_exit(fn -> File.chmod!(locked, 0o755) end)

      assert {:error, :eacces} = Filesystem.rename(src, Path.join(locked, "dst"))

      File.chmod!(locked, 0o755)
    end
  end
end
