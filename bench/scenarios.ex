defmodule RaftExBench.Scenarios do
  @moduledoc false

  alias RaftEx.Domain.Machine.Simple
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Cluster
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Infra.Persistence.Log
  alias RaftEx.Infra.Persistence.LogWal

  @scenarios %{
    "wal_append_1uid" => %{metric: :throughput, unit: "appends/s", sync: :datasync},
    "wal_append_10uids" => %{metric: :throughput, unit: "appends/s", sync: :datasync},
    "wal_append_group" => %{metric: :throughput, unit: "appends/s", sync: :datasync},
    "wal_recovery" => %{metric: :latency, unit: "us", sync: :none},
    "log_fold" => %{metric: :throughput, unit: "entries/s", sync: :none},
    "apply_committed" => %{metric: :throughput, unit: "entries/s", sync: :none},
    "replication_batch" => %{metric: :throughput, unit: "entries/s", sync: :none},
    "query" => %{metric: :throughput, unit: "queries/s", sync: :none}
  }

  @spec names() :: [String.t()]
  def names, do: @scenarios |> Map.keys() |> Enum.sort()

  @spec metric(String.t()) :: :throughput | :latency
  def metric(name), do: fetch!(name).metric

  @spec unit(String.t()) :: String.t()
  def unit(name), do: fetch!(name).unit

  @spec default_sync(String.t()) :: atom()
  def default_sync(name), do: fetch!(name).sync

  @doc "Run one scenario once and return `{elapsed_microseconds, input_size}`."
  @spec run(String.t(), map()) :: {non_neg_integer(), pos_integer()}
  def run(name, opts) do
    opts = if Map.get(opts, :sync), do: opts, else: Map.put(opts, :sync, default_sync(name))
    dir = tmp_dir()
    opts = Map.put(opts, :tmp_dir, dir)

    try do
      case name do
        "wal_append_1uid" -> wal_append(1, scaled(opts, 1_000), opts, 1)
        "wal_append_10uids" -> wal_append(10, scaled(opts, 50), opts, 5)
        "wal_append_group" -> wal_append(1, scaled(opts, 1_000), opts, 10)
        "wal_recovery" -> wal_recovery(round_to_100(scaled(opts, 10_000)), opts)
        "log_fold" -> log_fold(scaled(opts, 100_000), opts)
        "apply_committed" -> apply_committed(scaled(opts, 50_000), opts)
        "replication_batch" -> replication_batch(scaled(opts, 100_000), opts)
        "query" -> query(scaled(opts, 1_000), opts)
        other -> raise ArgumentError, "unknown benchmark scenario: #{inspect(other)}"
      end
    after
      File.rm_rf!(dir)
    end
  end

  # ---------------------------------------------------------------------------
  # WAL
  # ---------------------------------------------------------------------------

  defp wal_append(uid_count, per_uid, opts, concurrency) do
    dir = Map.fetch!(opts, :tmp_dir)
    name = unique_wal_name()
    sync = sync_method(opts)
    per_worker = max(1, div(per_uid, concurrency))

    config = %{
      names: %{wal: name},
      data_dir: dir,
      wal_compute_checksums: true,
      wal_sync_method: sync,
      wal_max_batch_size: 8192
    }

    {:ok, pid} = LogWal.start_link(config)

    try do
      start = System.monotonic_time(:microsecond)

      tasks =
        for uid <- 1..uid_count, worker <- 1..concurrency do
          offset = (worker - 1) * per_worker
          Task.async(fn -> append_entries(name, :"uid_#{uid}", per_worker, offset) end)
        end

      Task.await_many(tasks, :infinity)

      elapsed = System.monotonic_time(:microsecond) - start
      {elapsed, uid_count * concurrency * per_worker}
    after
      GenServer.stop(pid)
    end
  end

  defp append_entries(name, uid, count, offset) do
    Enum.each(1..count, fn n ->
      index = offset + n
      {:ok, _first, ^index} = LogWal.append(name, uid, [{index, 1, {:cmd, index}}])
    end)
  end

  defp wal_recovery(records, opts) do
    dir = Map.fetch!(opts, :tmp_dir)
    name = unique_wal_name()
    uid = :recovery_uid

    config = %{
      names: %{wal: name},
      data_dir: dir,
      wal_compute_checksums: true,
      wal_sync_method: :none,
      wal_max_batch_size: 8192
    }

    {:ok, pid} = LogWal.start_link(config)

    try do
      1..records
      |> Enum.chunk_every(100)
      |> Enum.each(fn chunk ->
        entries = Enum.map(chunk, fn index -> {index, 1, {:cmd, index}} end)
        {:ok, _, _} = LogWal.append(name, uid, entries)
      end)
    after
      GenServer.stop(pid)
    end

    start = System.monotonic_time(:microsecond)

    {first, last, entries} =
      LogWal.recover(%{data_dir: dir, uid: uid, wal_compute_checksums: true})

    elapsed = System.monotonic_time(:microsecond) - start
    ^records = length(entries)
    ^first = 1
    ^last = records
    {elapsed, records}
  end

  # ---------------------------------------------------------------------------
  # Log and server
  # ---------------------------------------------------------------------------

  defp log_fold(n, opts) do
    log = log_state(n)
    from = max(1, n - 99)
    to = n
    fun = fn _entry, acc -> acc + 1 end

    start = System.monotonic_time(:microsecond)
    {count, _log} = Log.fold(from, to, fun, 0, log)
    elapsed = System.monotonic_time(:microsecond) - start

    ^count = to - from + 1
    _ = opts
    {elapsed, count}
  end

  defp apply_committed(n, opts) do
    batch = min(100, max(1, n - 1))
    apply_to = n
    last_applied = apply_to - batch
    log = log_state(n)
    state = server_state(log, last_applied)

    start = System.monotonic_time(:microsecond)
    {new_state, _effects} = Server.apply_committed(apply_to, state)
    elapsed = System.monotonic_time(:microsecond) - start

    ^apply_to = new_state.last_applied
    _ = opts
    {elapsed, batch}
  end

  defp replication_batch(n, opts) do
    calls = max(1, round(10 * scale(opts)))
    batch = min(100, max(1, n - 1))
    log = log_state(n)
    state = leader_state(log, n, batch)

    start = System.monotonic_time(:microsecond)

    Enum.each(1..calls, fn _ ->
      {_state, effects} = Server.make_rpcs(state)
      3 = length(effects)
    end)

    elapsed = System.monotonic_time(:microsecond) - start
    {elapsed, calls * 3}
  end

  defp query(n, opts) do
    queries = max(1, round(1_000 * scale(opts)))
    log = log_state(n)
    state = query_state(log, n)
    fun = fn _ctx, machine_state -> machine_state end

    start = System.monotonic_time(:microsecond)

    Enum.each(1..queries, fn _ ->
      {:leader, _state, _effects} = Server.handle_leader({:local_query, fun}, state)
    end)

    elapsed = System.monotonic_time(:microsecond) - start
    {elapsed, queries}
  end

  # ---------------------------------------------------------------------------
  # State builders
  # ---------------------------------------------------------------------------

  defp log_state(n) do
    entries =
      Map.new(1..n, fn index ->
        {index, {index, 1, {:"$usr", %{ts: 0}, {:inc, index}, :"$ra_no_reply"}}}
      end)

    %Log{
      cfg: %{uid: :bench},
      range: {1, n},
      current_snapshot: nil,
      last_term: 1,
      last_written_index_term: {n, 1},
      next_index: n + 1,
      mem_table: nil,
      data_dir: System.tmp_dir!(),
      entries: entries
    }
  end

  defp server_state(log, last_applied) do
    fun = fn _cmd, state -> state + 1 end

    %{
      cfg: server_cfg(),
      leader_id: nil,
      current_term: 1,
      cluster: %{},
      commit_index: log.range |> elem(1),
      last_applied: last_applied,
      commit_latency: 0,
      log: log,
      machine_state: {:simple, fun, 0}
    }
  end

  defp leader_state(log, n, batch) do
    self = {:bench, node()}
    peers = [{:peer1, node()}, {:peer2, node()}, {:peer3, node()}]

    cluster =
      Enum.reduce(peers, Cluster.new(self, peers), fn peer, acc ->
        Cluster.update_peer(
          peer,
          %{next_index: n - batch + 1, match_index: n - batch},
          acc
        )
      end)

    %{
      cfg: %{server_cfg() | id: self, max_append_entries_rpc_batch_size: batch},
      current_term: 1,
      cluster: cluster,
      commit_index: n,
      last_applied: n,
      log: log,
      machine_state: {:simple, fn _cmd, state -> state end, 0}
    }
  end

  defp query_state(log, n) do
    %{
      cfg: server_cfg(),
      leader_id: nil,
      current_term: 1,
      cluster: %{},
      commit_index: n,
      last_applied: n,
      log: log,
      machine_state: {:simple, fn _ctx, state -> state end, 0}
    }
  end

  defp server_cfg do
    machine = {:machine, Simple, %{}}

    %Config{
      id: {:bench, node()},
      uid: :bench,
      log_id: "bench",
      metrics_key: :bench,
      machine: machine,
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: Simple,
      system_config: %{}
    }
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp fetch!(name) do
    case Map.fetch(@scenarios, name) do
      {:ok, scenario} -> scenario
      :error -> raise ArgumentError, "unknown benchmark scenario: #{inspect(name)}"
    end
  end

  defp scale(opts), do: Map.get(opts, :scale, 1.0)

  defp scaled(opts, base) do
    base
    |> Kernel.*(scale(opts))
    |> round()
    |> max(1)
  end

  defp round_to_100(n) do
    n
    |> div(100)
    |> max(1)
    |> Kernel.*(100)
  end

  defp sync_method(opts) do
    case Map.get(opts, :sync) do
      nil -> :none
      :fsync -> :fsync
      :datasync -> :datasync
      :none -> :none
      other -> raise ArgumentError, "unknown sync method: #{inspect(other)}"
    end
  end

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "raft_ex_bench_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    dir
  end

  defp unique_wal_name do
    :"raft_ex_bench_wal_#{System.unique_integer([:positive])}"
  end
end
