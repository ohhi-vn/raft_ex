# Design

## Context

See `proposal.md` for motivation and the spec deltas for required behavior.

Current constraints found in the code:

- `LogWal` is one GenServer per system serving every server uid. Each uid owns a file, a buffer list, and pending-write set.
- A synchronous append always flushes and syncs inside the caller's `GenServer.call`, so one command costs one write plus one sync.
- The buffer is grown with `buffer ++ entries`, and the flush path calls `:erlang.term_to_binary/1` twice for the same batch.
- Recovery appends each decoded record with `acc ++ decoded`, which is quadratic in record count.
- `Log.fold/6` filters and sorts the entire entries map for every replication batch and every commit-apply batch.
- `RpcHandler.append_entries/3` calls `Log.append/2` per entry, then `Log.write/2` appends the same entries again; it also calls `Log.truncate/2` once per new entry, and each truncation rewrites the whole WAL file.
- Metrics updates do `:ets.lookup` plus `:ets.insert` and call `Metrics.init/0` on every update.
- `LogSegmentWriter.recover/1` reads every segment file in full even when its sidecar index is present.
- `LogMeta.store_sync/4` fsyncs DETS on every call; `persist_last_applied/1` calls it whenever applied progress advances.

## Goals / Non-Goals

**Goals:**

- Make append, replication-read, and commit-apply work proportional to the batch, not the log.
- Make WAL flush and recovery linear in the number of records/entries.
- Remove duplicate serialization, duplicate appends, and repeated truncations.
- Keep all existing Raft safety and durability guarantees; acknowledged means durable.
- Produce benchmark evidence that can be re-run and compared.

**Non-Goals:**

- Changing the WAL file format or snapshot format.
- Changing public APIs or Raft protocol behavior.
- Introducing a new runtime dependency.
- Distributed-cluster RPC transport optimization beyond batch construction.
- Tuning OS/filesystem settings.

## Decisions

### 1. WAL buffer as a reversed list with an explicit size

Store the pending buffer newest-first and append a request with `Enum.reverse(entries, buffer)`; flush once with `Enum.reverse/1`. Track `buffer_size` in the file state so batch-limit checks and `info/2` are O(1).

Alternative considered: `:queue`. It avoids the final reverse but allocates more per join and complicates range reads. A reversed list plus one reverse per flush is simpler and still linear.

### 2. WAL group commit with deferred flush

A synchronous append enqueues entries plus `{from, first_index, last_index}` and returns `{:noreply, state}`; a zero-delay self-message (guarded by a `flush_scheduled?` flag) performs one write and one sync for all queued callers, then replies to each caller with its own range. Size-triggered and timer-triggered flushes use the same path. `append_async` callers join the same pending set and receive `{:wal_written, first, last}` after the batch is durable.

Alternatives considered:

- Keep one sync per call: rejected, it is the throughput ceiling.
- Ack before sync and sync on a timer: rejected, it breaks "acknowledged means durable".
- A separate flusher process: rejected, it adds a hop and lifecycle complexity for no gain over deferred self-messages.

Open the WAL file without `:delayed_write`. Group commit already provides batching, and removing delayed write makes `datasync`/`fsync` semantics unambiguous.

On write or sync failure, reply `{:error, reason}` to every queued caller and leave the buffer in place for retry; do not report success.

### 3. Serialize each WAL batch once

`flush_buffer/2` builds `entries_binary` once, computes `byte_size/1`, reuses it for the header, CRC, and file write, and passes the same size to the metrics update. `write_entries_to_wal/3` no longer calls `term_to_binary/1`.

### 4. Linear WAL recovery

Recovery decodes records into a reversed accumulator (`Enum.reverse(decoded, acc)`) and reverses once when the file ends. Incomplete trailing records stop the scan as today. `truncate_from/3` reuses the same linear recovery, then rewrites the retained entries in one pass.

### 5. Range-based log fold

`Log.fold/6` walks `from..to` and looks each index up in the entries map, skipping absent indexes. This is O(range) instead of O(total entries log total entries). Add a fallback: when the requested range is much larger than the map (sparse or heavily compacted log), iterate the map and filter, which is cheaper than probing a huge empty range.

`Log.write/2` batches the mem-table insert into one `:ets.insert/2` call and merges the new entries into the map in one pass.

`compute_last_term/2` uses the recovered WAL last index instead of `Enum.max_by/2` over all entries. Truncation derives the new last index from `from_index - 1` (probing down only across actual gaps) and removes keys with `Map.drop/2` instead of rejecting over the whole map.

### 6. One truncate and one write per follower AppendEntries

`RpcHandler.append_entries/3` finds the first entry whose index is missing or has a different term. If that index is below the request's last index, it truncates once at that index; then it writes all entries from that index onward with a single `Log.write/2`. Entries already present with matching terms are skipped. This removes the per-entry truncation (each currently rewrites the WAL) and the double append (in-memory append followed by `Log.write/2` appending again).

The same batch-shaped write path is used for the leader's pipelined replication reads: `Log.fold/6` returns only the requested range.

### 7. Atomic ETS metrics

Counters use `:ets.update_counter/4` with a default tuple, which is one atomic BIF and loses no increments. `Metrics.init/0` is called only on the error path: the update is attempted first and an `ArgumentError` triggers table creation and one retry. Table creation tolerates a concurrent creator by rescuing the `ArgumentError` when the table now exists.

Histograms update count and sum atomically with `update_counter` on two positions; min and max remain best-effort read-modify-write because ETS has no atomic min/max. The spec only requires count and sum to be exact. Gauges use `:ets.insert/2` directly.

Alternative considered: a GenServer owning all metrics. Rejected: it serializes every server through one mailbox.

### 8. Group-commit metadata syncs

`LogMeta.store_sync/4` keeps its durability contract but batches concurrent callers: write ETS and DETS immediately, queue the caller, sync once for the queued group, then reply to all. This preserves "reply after durable" while sharing one DETS sync across servers that advance in the same interval.

Alternative considered: make `persist_last_applied/1` asynchronous. Rejected for now because the changelog documents a deliberate synchronous guarantee; batching keeps it.

### 9. Lazy segment metadata at startup

`LogSegmentWriter.recover/1` reads only the fixed header with `:file.pread/3` and loads the sidecar index when present. It reads and parses the full segment only when the sidecar is missing. `append_entries/2` builds the index with a reversed accumulator instead of `idx ++ new_index_entries`.

### 10. Benchmark harness

Add `scripts/bench.exs` (run with `mix run scripts/bench.exs`) with scenario modules under `bench/`:

- `wal_append` (1 uid and 10 uids, batch sizes 1 and 100)
- `wal_recovery` (10k and 100k records)
- `log_fold` (batch 100 over a 100k-entry log)
- `apply_committed` (batch 100)
- `replication_batch` (3-peer effect construction over a 100k-entry log)
- `query` (local query against an applied state machine)

Each scenario uses a fresh temp directory, runs warm-up iterations, then measures a fixed iteration count. Output is a table plus JSON with scenario, iterations, elapsed, throughput/latency, input size, timestamp, git revision, OTP and Elixir versions.

`--record` writes `bench/baseline.json`; `--compare <file> --threshold <percent>` prints deltas and exits non-zero when a scenario regresses beyond the threshold. Results and the exact command are recorded in `docs/performance.md`.

Alternative considered: Benchee. Rejected to avoid a new dependency and because baseline comparison, exit codes, and stable JSON are straightforward to implement directly.

## Risks / Trade-offs

- **Group commit changes latency shape** → Each caller waits for one shared sync instead of its own. Throughput rises; tail latency under light load is roughly unchanged because the flush is scheduled immediately. Add an explicit `LogWal.flush/1` for tests and operators.
- **Deferred flush could delay acknowledgement if the self-message is lost** → The periodic flush timer remains as a backstop, and `flush_scheduled?` prevents duplicate scheduling.
- **Error replies are new behavior** → Existing tests that assume success on a broken WAL must be updated; this aligns with the new durable-append spec.
- **Range fold can be slower on sparse logs** → The map-size fallback covers heavily compacted or gapped logs.
- **Histogram min/max remain approximate under concurrency** → Count and sum are exact; document min/max as best-effort. If exact extremes are needed later, add a per-shard sketch.
- **Removing `:delayed_write` may reduce single-call throughput** → Group commit more than compensates, and it removes a durability ambiguity.
- **Benchmark variance** → Use warm-up, fixed seeds, and local baselines; do not gate CI on thresholds until a dedicated runner exists.

## Migration Plan

1. Land the WAL changes behind the existing API; the on-disk format is unchanged, so existing data directories remain readable.
2. Land log/read/apply and RPC changes with the existing test suite green.
3. Land metrics and metadata batching.
4. Add the benchmark harness, record the baseline on the target machine, and document results.
5. Rollback is a code revert; WAL and snapshot files written by either version are compatible.

## Open Questions

- Whether benchmark comparison should run on every CI build or only on a dedicated performance runner. The command and threshold support both; this can be decided when CI capacity is known.
