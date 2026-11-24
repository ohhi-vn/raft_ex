defmodule RaftEx.Infra.Persistence.LogWalTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogWal

  @uid :test_server_1

  setup do
    data_dir = RaftEx.TestSupport.tmp_dir("log_wal_test")
    wal_name = :"log_wal_test_#{System.unique_integer([:positive])}"
    Process.put(:log_wal_test_name, wal_name)
    Process.put(:log_wal_test_dir, data_dir)

    config = %{
      names: %{wal: wal_name},
      data_dir: data_dir,
      wal_compute_checksums: true
    }

    start_supervised!({LogWal, config})

    %{wal_name: wal_name, data_dir: data_dir, config: config}
  end

  defp wal_name, do: Process.get(:log_wal_test_name)
  defp uid, do: @uid

  describe "append/4 and read/4" do
    test "appends and reads entries by index range" do
      entries = [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}]
      assert {:ok, 0, 3} = LogWal.append(wal_name(), uid(), entries)

      # append/4 flushes immediately, so the in-memory buffer is empty
      assert LogWal.read(wal_name(), uid(), 1, 3) == []
    end

    test "append_async keeps entries in the buffer until flush" do
      assert :ok = LogWal.append_async(wal_name(), uid(), [{1, 1, :a}, {2, 1, :b}])
      assert LogWal.read(wal_name(), uid(), 1, 2) == [{1, 1, :a}, {2, 1, :b}]
    end
  end

  describe "truncate/4" do
    test "removes entries from an index onward" do
      LogWal.append(wal_name(), uid(), [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}])
      assert :ok = LogWal.truncate(wal_name(), uid(), 2)
      assert LogWal.read(wal_name(), uid(), 1, 3) == []
    end
  end

  describe "info/2 and overview/1" do
    test "report WAL state" do
      assert {0, 0, 0} = LogWal.info(wal_name(), uid())
      assert is_map(LogWal.overview(wal_name()))

      LogWal.append(wal_name(), uid(), [{1, 1, :a}])
      assert {0, 1, _size} = LogWal.info(wal_name(), uid())
    end
  end

  describe "multiple servers" do
    test "each uid keeps its own file and index range" do
      LogWal.append(wal_name(), :server_a, [{1, 1, :a}, {2, 1, :b}])
      LogWal.append(wal_name(), :server_b, [{1, 1, :x}])

      assert {0, 2, 0} = LogWal.info(wal_name(), :server_a)
      assert {0, 1, 0} = LogWal.info(wal_name(), :server_b)
    end

    test "recovers each uid from its own file" do
      config = %{
        names: %{wal: wal_name()},
        data_dir: Process.get(:log_wal_test_dir),
        wal_compute_checksums: true
      }

      LogWal.append(wal_name(), :server_a, [{1, 1, :a}, {2, 1, :b}])
      LogWal.append(wal_name(), :server_b, [{1, 1, :x}])
      GenServer.stop(Process.whereis(wal_name()))

      assert {1, 2, [{1, 1, :a}, {2, 1, :b}]} =
               LogWal.recover(%{data_dir: Path.join(config.data_dir, "server_a")})

      assert {1, 1, [{1, 1, :x}]} =
               LogWal.recover(%{data_dir: Path.join(config.data_dir, "server_b")})
    end
  end

  describe "recover/1" do
    test "recovers entries from an existing WAL file" do
      dir = RaftEx.TestSupport.tmp_dir("wal_recover")
      name = :"log_wal_recover_#{System.unique_integer([:positive])}"

      config = %{
        names: %{wal: name},
        data_dir: dir,
        wal_compute_checksums: true
      }

      {:ok, pid} = LogWal.start_link(config)

      LogWal.append(name, uid(), [{1, 1, :a}, {2, 1, :b}])
      GenServer.stop(pid)

      assert {1, 2, [{1, 1, :a}, {2, 1, :b}]} =
               LogWal.recover(%{data_dir: Path.join(dir, Atom.to_string(uid()))})
    end
  end
end
