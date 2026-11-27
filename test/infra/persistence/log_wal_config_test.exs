defmodule RaftEx.Infra.Persistence.LogWalConfigTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogWal

  test "normalize_config honors canonical keys, aliases, and defaults" do
    assert %{wal_sync_method: :datasync, wal_compute_checksums: true, wal_max_batch_size: 8192} =
             LogWal.normalize_config(%{})

    supervisor_style = %{
      sync_method: :fsync,
      compute_checksums: false,
      max_batch_size: 7,
      max_size_bytes: 99_999,
      max_entries: 11,
      dir: "/tmp/x"
    }

    normalized = LogWal.normalize_config(supervisor_style)
    assert normalized.wal_sync_method == :fsync
    assert normalized.wal_compute_checksums == false
    assert normalized.wal_max_batch_size == 7
    assert normalized.wal_max_size_bytes == 99_999
    assert normalized.wal_max_entries == 11
    # Unrelated keys pass through.
    assert normalized.dir == "/tmp/x"

    # Canonical keys win when both styles are present.
    both =
      LogWal.normalize_config(%{
        wal_sync_method: :none,
        sync_method: :fsync,
        wal_max_batch_size: 3,
        max_batch_size: 50
      })

    assert both.wal_sync_method == :none
    assert both.wal_max_batch_size == 3
  end

  test "a server started with supervisor-style keys stores and honors them" do
    base = RaftEx.TestSupport.tmp_dir("wal_config")
    name = :"config_wal_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      LogWal.start_link(%{
        names: %{wal: name},
        data_dir: base,
        sync_method: :fsync,
        compute_checksums: false,
        max_batch_size: 7,
        max_size_bytes: 99_999
      })

    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    state = :sys.get_state(pid)
    assert state.config.wal_sync_method == :fsync
    assert state.config.wal_compute_checksums == false
    assert state.config.wal_max_batch_size == 7
    assert state.config.wal_max_size_bytes == 99_999

    # The configured sync method is actually used: appends succeed under each
    # supported method.
    assert {:ok, 1, 1} = LogWal.append(name, "uid-sync", [{1, 1, :a}])
  end

  test "each supported sync method completes appends" do
    for method <- [:fsync, :datasync, :none] do
      base = RaftEx.TestSupport.tmp_dir("wal_sync_#{method}")
      name = :"sync_wal_#{method}_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        LogWal.start_link(%{names: %{wal: name}, data_dir: base, wal_sync_method: method})

      Process.unlink(pid)

      assert {:ok, 1, 1} = LogWal.append(name, "uid", [{1, 1, :a}])

      state = :sys.get_state(pid)
      assert state.config.wal_sync_method == method
      GenServer.stop(pid)
    end
  end

  test "checksums disabled still recovers" do
    base = RaftEx.TestSupport.tmp_dir("wal_nochecksum")
    name = :"nochecksum_wal_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      LogWal.start_link(%{names: %{wal: name}, data_dir: base, wal_compute_checksums: false})

    Process.unlink(pid)
    assert {:ok, 1, 2} = LogWal.append(name, "uid", [{1, 1, :a}, {2, 1, :b}])
    GenServer.stop(pid)

    {_first, last, recovered} =
      LogWal.recover(%{data_dir: Path.join(base, "uid"), wal_compute_checksums: false})

    assert last == 2
    assert length(recovered) == 2
  end
end
