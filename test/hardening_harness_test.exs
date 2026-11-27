defmodule RaftEx.HardeningHarnessTest do
  @moduledoc """
  Harness verification for tasks 1.3 and 1.4.

  Exercises the hard-kill/restart helpers, WAL fault injection, the
  deterministic multi-chunk snapshot fixture, and the duplicate/reordered
  RPC interceptor with trivial passing tests. Full fault-coverage tests
  land in later phases; these prove the helpers work.
  """
  use ExUnit.Case, async: false

  alias RaftEx.TestSupport.{FaultInjection, RpcInterceptor, SnapshotFixture, WalFault}

  test "1.3 hard_kill_and_wait kills a process" do
    pid = spawn(fn -> Process.sleep(:infinity) end)
    assert Process.alive?(pid)
    assert :ok = FaultInjection.hard_kill_and_wait(pid)
    refute Process.alive?(pid)
  end

  test "1.3 WAL fault helpers corrupt and truncate a file" do
    dir = RaftEx.TestSupport.tmp_dir("wal_fault_harness")
    path = Path.join(dir, "wal.dat")
    File.write!(path, :erlang.term_to_binary(%{a: 1}) <> "PAYLOAD1234567890")

    assert :ok = WalFault.append_garbage(path)
    {:ok, with_garbage} = File.read(path)
    assert String.ends_with?(with_garbage, "GARBAGE")

    assert :ok = WalFault.truncate_file(path, 10)
    {:ok, truncated} = File.read(path)
    assert byte_size(truncated) == 10

    File.write!(path, "0123456789ABCDEF")
    assert :ok = WalFault.corrupt_byte(path, 0)
    {:ok, corrupted} = File.read(path)
    refute corrupted == "0123456789ABCDEF"
  end

  test "1.3 failing WAL server rejects appends" do
    pid = WalFault.start_failing_wal(:enospc)
    assert {:error, :enospc} = GenServer.call(pid, {:append, "uid", []})
    assert {:error, :enospc} = GenServer.call(pid, {:truncate, "uid", 1})
  end

  test "1.4 snapshot fixture exceeds one chunk" do
    chunk_size = 65_536
    payload = SnapshotFixture.default()
    assert byte_size(payload) > chunk_size

    # Deterministic: same size twice.
    assert SnapshotFixture.large_state(100_000) == SnapshotFixture.large_state(100_000)
  end

  test "1.4 RPC interceptor duplicates and reorders" do
    assert [:a, :a] = RpcInterceptor.duplicate(:a)
    assert [:b, :a, :c] = RpcInterceptor.reorder([:a, :b, :c])
    assert [] = RpcInterceptor.reorder([])

    rec = RpcInterceptor.start_recorder()
    RpcInterceptor.deliver(rec, :first)
    RpcInterceptor.deliver(rec, :second)
    assert [:first, :second] = RpcInterceptor.recorded(rec)
  end
end
