defmodule RaftEx.Infra.Persistence.LogWalTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Observability.Metrics
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
      assert {:ok, 1, 3} = LogWal.append(wal_name(), uid(), entries)

      # append/4 flushes immediately, so the in-memory buffer is empty
      assert LogWal.read(wal_name(), uid(), 1, 3) == []
    end

    test "append_async keeps entries in the buffer until flush" do
      assert :ok = LogWal.append_async(wal_name(), uid(), [{1, 1, :a}, {2, 1, :b}])
      assert LogWal.read(wal_name(), uid(), 1, 2) == [{1, 1, :a}, {2, 1, :b}]
    end

    test "keeps buffered entries in append order across batches" do
      assert :ok = LogWal.append_async(wal_name(), uid(), [{1, 1, :a}, {2, 1, :b}])
      assert :ok = LogWal.append_async(wal_name(), uid(), [{3, 1, :c}, {4, 1, :d}])

      assert LogWal.read(wal_name(), uid(), 1, 4) ==
               [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}, {4, 1, :d}]

      assert {:ok, 5, 5} = LogWal.append(wal_name(), uid(), [{5, 1, :e}])

      assert {1, 5, entries} =
               LogWal.recover(%{
                 data_dir: Path.join(Process.get(:log_wal_test_dir), Atom.to_string(uid()))
               })

      assert entries == [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}, {4, 1, :d}, {5, 1, :e}]
    end
  end

  describe "buffer accounting" do
    test "reports the exact buffered entry count" do
      assert {0, 0, 0} = LogWal.info(wal_name(), uid())

      assert :ok = LogWal.append_async(wal_name(), uid(), [{1, 1, :a}])
      assert {0, 1, 1} = LogWal.info(wal_name(), uid())

      assert :ok = LogWal.append_async(wal_name(), uid(), [{2, 1, :b}, {3, 1, :c}])
      assert {0, 3, 3} = LogWal.info(wal_name(), uid())
      assert LogWal.overview(wal_name()).buffer_size == 3
    end

    test "flushes as soon as the batch limit is reached" do
      dir = RaftEx.TestSupport.tmp_dir("wal_buffer_limit")
      name = :"wal_buffer_limit_#{System.unique_integer([:positive])}"

      start_supervised!(%{
        id: name,
        start:
          {LogWal, :start_link, [%{names: %{wal: name}, data_dir: dir, wal_max_batch_size: 3}]}
      })

      assert :ok = LogWal.append_async(name, :limit_uid, [{1, 1, :a}, {2, 1, :b}])
      assert {0, 2, 2} = LogWal.info(name, :limit_uid)

      assert :ok = LogWal.append_async(name, :limit_uid, [{3, 1, :c}])
      assert_receive {:wal_written, 0, 3}
      assert {0, 3, 0} = LogWal.info(name, :limit_uid)

      assert :ok = LogWal.append_async(name, :limit_uid, [{4, 1, :d}])
      assert {0, 4, 1} = LogWal.info(name, :limit_uid)
    end
  end

  describe "truncate/4" do
    test "removes entries from an index onward" do
      LogWal.append(wal_name(), uid(), [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}])
      assert :ok = LogWal.truncate(wal_name(), uid(), 2)
      assert LogWal.read(wal_name(), uid(), 1, 3) == []
    end
  end

  describe "truncate_from/4" do
    test "removes entries at and after an index from disk" do
      assert {:ok, 1, 3} =
               LogWal.append(wal_name(), uid(), [
                 {1, 1, :a},
                 {2, 1, :b},
                 {3, 1, :c}
               ])

      assert :ok = LogWal.truncate_from(wal_name(), uid(), 2)

      assert {1, 1, _} = LogWal.info(wal_name(), uid())

      assert [{1, 1, :a}] =
               LogWal.recover(%{data_dir: Process.get(:log_wal_test_dir), uid: uid()}) |> elem(2)
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

  describe "edge cases" do
    test "unknown servers are harmless for truncate, truncate_from, read and info" do
      unknown = :"never_opened_#{System.unique_integer([:positive])}"

      assert :ok = LogWal.truncate(wal_name(), unknown, 3)
      assert :ok = LogWal.truncate_from(wal_name(), unknown, 3)
      assert LogWal.read(wal_name(), unknown, 1, 3) == []
      assert {0, 0, 0} = LogWal.info(wal_name(), unknown)
    end

    test "overview aggregates active files and pending asynchronous writes" do
      assert {:ok, 1, 1} = LogWal.append(wal_name(), uid(), [{1, 1, :a}])
      assert :ok = LogWal.append_async(wal_name(), :overview_uid, [{5, 1, :e}])

      overview = LogWal.overview(wal_name())

      assert overview.files >= 2
      assert overview.first_index == 0
      assert overview.last_index == 5
      assert overview.buffer_size >= 1
      assert overview.pending_writes >= 1
    end

    test "appending an empty batch is a no-op" do
      assert {:ok, 0, 0} = LogWal.append(wal_name(), uid(), [])
      assert {0, 0, 0} = LogWal.info(wal_name(), uid())
    end

    test "asynchronous appends flush and notify when the batch limit is reached" do
      dir = RaftEx.TestSupport.tmp_dir("wal_batch_flush")
      name = :"wal_batch_#{System.unique_integer([:positive])}"

      start_supervised!(%{
        id: name,
        start:
          {LogWal, :start_link, [%{names: %{wal: name}, data_dir: dir, wal_max_batch_size: 2}]}
      })

      assert :ok = LogWal.append_async(name, :batch_uid, [{1, 1, :a}, {2, 1, :b}])
      assert_receive {:wal_written, 0, 2}
      assert {0, 2, 0} = LogWal.info(name, :batch_uid)
    end

    test "all sync methods can be used" do
      for method <- [:fsync, :none, :unsupported_sync_method] do
        dir = RaftEx.TestSupport.tmp_dir("wal_sync_#{method}")
        name = :"wal_sync_#{method}_#{System.unique_integer([:positive])}"

        {:ok, pid} =
          LogWal.start_link(%{names: %{wal: name}, data_dir: dir, wal_sync_method: method})

        assert {:ok, 1, 1} = LogWal.append(name, uid(), [{1, 1, :a}])
        GenServer.stop(pid)
      end
    end

    test "truncation keeps entries still buffered in memory" do
      assert :ok = LogWal.append_async(wal_name(), :truncated_uid, [{1, 1, :a}, {2, 1, :b}])
      assert :ok = LogWal.truncate(wal_name(), :truncated_uid, 0)

      assert {1, 2, 2} = LogWal.info(wal_name(), :truncated_uid)
      assert LogWal.read(wal_name(), :truncated_uid, 1, 2) == [{1, 1, :a}, {2, 1, :b}]
    end

    test "a synchronous append notifies earlier asynchronous writers" do
      assert :ok = LogWal.append_async(wal_name(), :notify_uid, [{1, 1, :a}])
      assert {:ok, 2, 2} = LogWal.append(wal_name(), :notify_uid, [{2, 1, :b}])

      assert_receive {:wal_written, 0, 2}
    end

    test "overview falls back to a zeroed map when the WAL is not running" do
      name = :"missing_wal_#{System.unique_integer([:positive])}"

      assert LogWal.overview(name) == %{
               files: 0,
               first_index: 0,
               last_index: 0,
               buffer_size: 0,
               pending_writes: 0
             }
    end
  end

  describe "write failures and direct callbacks" do
    test "a failed write leaves the file unchanged and keeps the buffer for retry" do
      config = %{
        names: %{wal: :"wal_direct_#{System.unique_integer([:positive])}"},
        data_dir: RaftEx.TestSupport.tmp_dir("wal_direct_write")
      }

      {:ok, state} = LogWal.init(config)

      # A deferred (nil `from`) append buffers entries without flushing.
      {:reply, {:ok, 0, 1}, state} =
        LogWal.handle_call({:append, :direct_uid, [{1, 1, :a}]}, nil, state)

      file = Map.fetch!(state.files, :direct_uid)
      {:ok, read_only_fd} = :file.open(file.path, [:read, :binary, :raw])
      state = %{state | files: Map.put(state.files, :direct_uid, %{file | fd: read_only_fd})}

      {:reply, {:ok, 0, 2}, state} =
        LogWal.handle_call({:append, :direct_uid, [{2, 1, :b}]}, nil, state)

      # The flush fails because the fd is read-only; the buffer is retained.
      {:noreply, state} = LogWal.handle_info(:group_commit, state)

      assert state.files[:direct_uid].file_size == file.file_size
      assert state.files[:direct_uid].buffer_size == 2

      LogWal.terminate(:normal, state)
    end

    test "a non-pid reply target is ignored" do
      config = %{
        names: %{wal: :"wal_direct_#{System.unique_integer([:positive])}"},
        data_dir: RaftEx.TestSupport.tmp_dir("wal_direct_no_reply")
      }

      {:ok, state} = LogWal.init(config)

      {:reply, {:ok, 0, 1}, state} =
        LogWal.handle_call({:append, :direct_uid, [{1, 1, :a}]}, :not_a_pid, state)

      refute_receive {:wal_written, _, _}, 20
      LogWal.terminate(:normal, state)
    end
  end

  describe "group commit" do
    test "queued synchronous appends share one sync and each receives its own range" do
      config = %{
        names: %{wal: :"wal_group_#{System.unique_integer([:positive])}"},
        data_dir: RaftEx.TestSupport.tmp_dir("wal_group_commit"),
        wal_sync_method: :datasync
      }

      {:ok, state} = LogWal.init(config)
      Metrics.reset()

      # Keep one message queued so the first append defers instead of flushing
      # inline; this makes the grouping deterministic.
      send(self(), {:append, :any_uid, []})

      ref1 = make_ref()
      ref2 = make_ref()
      ref3 = make_ref()

      {:noreply, state} =
        LogWal.handle_call({:append, :group_uid, [{1, 1, :a}]}, {self(), ref1}, state)

      {:noreply, state} =
        LogWal.handle_call({:append, :group_uid, [{2, 1, :b}]}, {self(), ref2}, state)

      {:noreply, state} =
        LogWal.handle_call({:append, :group_uid, [{3, 1, :c}]}, {self(), ref3}, state)

      assert state.files[:group_uid].buffer_size == 3

      {:noreply, state} = LogWal.handle_info(:group_commit, state)

      assert_receive {^ref1, {:ok, 1, 1}}
      assert_receive {^ref2, {:ok, 2, 2}}
      assert_receive {^ref3, {:ok, 3, 3}}

      assert Metrics.get(:wal_fsyncs) == 1
      assert state.files[:group_uid].buffer_size == 0

      LogWal.terminate(:normal, state)
    end

    test "every queued caller receives the error when a flush fails" do
      config = %{
        names: %{wal: :"wal_group_error_#{System.unique_integer([:positive])}"},
        data_dir: RaftEx.TestSupport.tmp_dir("wal_group_error")
      }

      {:ok, state} = LogWal.init(config)

      # Force the first append to defer so all three callers are queued.
      send(self(), {:append, :any_uid, []})
      ref1 = make_ref()
      ref2 = make_ref()

      {:noreply, state} =
        LogWal.handle_call({:append, :error_uid, [{1, 1, :a}]}, {self(), ref1}, state)

      {:noreply, state} =
        LogWal.handle_call({:append, :error_uid, [{2, 1, :b}]}, {self(), ref2}, state)

      file = Map.fetch!(state.files, :error_uid)
      {:ok, read_only_fd} = :file.open(file.path, [:read, :binary, :raw])
      state = %{state | files: Map.put(state.files, :error_uid, %{file | fd: read_only_fd})}

      {:noreply, state} = LogWal.handle_info(:group_commit, state)

      assert_receive {^ref1, {:error, _reason}}
      assert_receive {^ref2, {:error, _reason}}

      # The buffer survives so a later flush can retry.
      assert state.files[:error_uid].buffer_size == 2
      assert state.files[:error_uid].pending_sync == []

      LogWal.terminate(:normal, state)
    end
  end

  describe "recovery at scale" do
    test "recovers ten thousand records in ascending order" do
      entries = Enum.map(1..10_000, fn idx -> {idx, 1, {:cmd, idx}} end)
      assert {:ok, 1, 10_000} = LogWal.append(wal_name(), uid(), entries)

      dir = Path.join(Process.get(:log_wal_test_dir), Atom.to_string(uid()))

      assert {1, 10_000, recovered} = LogWal.recover(%{data_dir: dir})
      assert length(recovered) == 10_000
      assert recovered == entries
    end

    test "stops at a torn trailing record and keeps complete records in order" do
      dir = RaftEx.TestSupport.tmp_dir("wal_torn_trailing")
      File.mkdir_p!(dir)
      path = Path.join(dir, "wal.dat")

      complete = :erlang.term_to_binary([{1, 1, :a}, {2, 1, :b}])

      header =
        <<0x5241574C::32-unsigned, 2::8-unsigned, 2::32-unsigned,
          byte_size(complete)::32-unsigned>>

      crc = :erlang.crc32([header, complete])
      torn = <<0x5241574C::32-unsigned, 2::8-unsigned, 1::32-unsigned, 100::32-unsigned, "short">>

      File.write!(path, [header, complete, <<crc::32-unsigned>>, torn])

      assert {1, 2, [{1, 1, :a}, {2, 1, :b}]} = LogWal.recover(%{data_dir: dir})
    end
  end

  describe "truncate_from/4 and restart" do
    test "a truncated suffix stays truncated after a restart" do
      assert {:ok, 1, 5} =
               LogWal.append(wal_name(), uid(), Enum.map(1..5, &{&1, 1, {:cmd, &1}}))

      assert :ok = LogWal.truncate_from(wal_name(), uid(), 4)

      restart_wal()

      assert {:ok, 4, 4} = LogWal.append(wal_name(), uid(), [{4, 2, :replacement}])
      assert {1, 4, 0} = LogWal.info(wal_name(), uid())

      dir = Path.join(Process.get(:log_wal_test_dir), Atom.to_string(uid()))

      assert {1, 4, entries} = LogWal.recover(%{data_dir: dir})
      assert Enum.map(entries, &elem(&1, 0)) == [1, 2, 3, 4]
      assert List.last(entries) == {4, 2, :replacement}
    end

    test "truncating the whole log leaves it empty after a restart" do
      assert {:ok, 1, 3} = LogWal.append(wal_name(), uid(), Enum.map(1..3, &{&1, 1, :x}))

      assert :ok = LogWal.truncate_from(wal_name(), uid(), 1)
      assert {1, 0, 0} = LogWal.info(wal_name(), uid())

      restart_wal()

      assert {:ok, 1, 1} = LogWal.append(wal_name(), uid(), [{1, 1, :fresh}])
      # The truncated file has no prior range, so the reopened WAL starts at 0.
      assert {0, 1, 0} = LogWal.info(wal_name(), uid())

      dir = Path.join(Process.get(:log_wal_test_dir), Atom.to_string(uid()))
      assert {1, 1, [{1, 1, :fresh}]} = LogWal.recover(%{data_dir: dir})
    end
  end

  defp restart_wal do
    config = %{
      names: %{wal: wal_name()},
      data_dir: Process.get(:log_wal_test_dir),
      wal_compute_checksums: true
    }

    :ok = stop_supervised(LogWal)
    start_supervised!({LogWal, config})
  end

  describe "corrupt and legacy WAL files" do
    defp write_raw_wal(path, entries_binary, opts \\ []) do
      version = Keyword.get(opts, :version, 2)
      count = Keyword.get(opts, :count, 1)
      checksums = Keyword.get(opts, :checksums, true)
      blob_size = byte_size(entries_binary)

      header =
        <<0x5241574C::32-unsigned, version::8-unsigned, count::32-unsigned,
          blob_size::32-unsigned>>

      data =
        if checksums do
          crc = :erlang.crc32([header, entries_binary])
          [header, entries_binary, <<crc::32-unsigned>>]
        else
          [header, entries_binary]
        end

      File.mkdir_p!(Path.dirname(path))
      File.write!(path, data)
      path
    end

    test "a WAL path that is a directory cannot be recovered" do
      dir = RaftEx.TestSupport.tmp_dir("wal_unreadable")
      File.mkdir_p!(Path.join([dir, Atom.to_string(uid()), "wal.dat"]))

      assert LogWal.recover(%{data_dir: dir, uid: uid()}) == {0, 0, []}
    end

    test "a truncated header stops recovery" do
      dir = RaftEx.TestSupport.tmp_dir("wal_truncated_header")
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "wal.dat"), <<"RAWL", 2>>)

      assert LogWal.recover(%{data_dir: dir}) == {0, 0, []}
    end

    test "a truncated record header stops recovery" do
      dir = RaftEx.TestSupport.tmp_dir("wal_truncated_record")
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "wal.dat"), <<"RAWL", 2, 1::32, 0::32, 0::32>>)

      assert LogWal.recover(%{data_dir: dir}) == {0, 0, []}
    end

    test "a record without a checksum footer is ignored" do
      dir = RaftEx.TestSupport.tmp_dir("wal_no_footer")

      write_raw_wal(Path.join(dir, "wal.dat"), :erlang.term_to_binary([{1, 1, :a}]),
        checksums: false
      )

      assert LogWal.recover(%{data_dir: dir}) == {0, 0, []}
    end

    test "checksums can be disabled during recovery" do
      dir = RaftEx.TestSupport.tmp_dir("wal_no_checksums")

      write_raw_wal(Path.join(dir, "wal.dat"), :erlang.term_to_binary([{1, 1, :a}]),
        checksums: false
      )

      assert LogWal.recover(%{data_dir: dir, wal_compute_checksums: false}) ==
               {1, 1, [{1, 1, :a}]}
    end

    test "version 1 records are not decoded" do
      dir = RaftEx.TestSupport.tmp_dir("wal_v1")
      write_raw_wal(Path.join(dir, "wal.dat"), :erlang.term_to_binary([{1, 1, :a}]), version: 1)

      assert LogWal.recover(%{data_dir: dir}) == {0, 0, []}
    end

    test "invalid term data is skipped" do
      dir = RaftEx.TestSupport.tmp_dir("wal_invalid_term")
      write_raw_wal(Path.join(dir, "wal.dat"), "not a term")

      assert LogWal.recover(%{data_dir: dir}) == {0, 0, []}
    end
  end
end
