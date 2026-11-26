# Run with: mix run scripts/wal_benchmark.exs
# Compares per-server (LogWal) append throughput for 1/10/50 uids.

alias RaftEx.Infra.Persistence.LogWal

for n <- [1, 10, 50] do
  dir = Path.join(System.tmp_dir!(), "wal_bench_#{n}_#{System.unique_integer([:positive])}")
  File.mkdir_p!(dir)

  config = %{
    names: %{wal: :"wal_bench_#{n}"},
    data_dir: dir,
    wal_compute_checksums: true
  }

  {:ok, pid} = LogWal.start_link(config)
  name = config.names.wal
  uids = Enum.map(1..n, &:"uid_#{&1}")

  start = System.monotonic_time(:millisecond)

  Enum.each(uids, fn uid ->
    LogWal.append(name, uid, [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}])
  end)

  elapsed = System.monotonic_time(:millisecond) - start
  GenServer.stop(pid)

  IO.puts("uids=#{n} appends=#{n * 3} entries elapsed=#{elapsed}ms")
  File.rm_rf!(dir)
end