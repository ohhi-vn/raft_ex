# Proposal

## Why

RaftEx's write path does work proportional to the entire log on every append, and its WAL serializes each batch twice and fsyncs once per synchronous call. These costs dominate command throughput and restart time, and the README still lists performance as planned. The project has no repeatable benchmark, so regressions are invisible.

## What Changes

- Make WAL buffering and recovery linear: replace `buffer ++ entries` with an amortized append structure, accumulate recovery results without repeated list concatenation, and size buffers in O(1).
- Serialize each WAL batch once and reuse the binary for both the write and the byte-count metric; remove the duplicate `:erlang.term_to_binary/1` call.
- Add WAL group commit: queue synchronous append callers and acknowledge all of them after a single batched write and sync, instead of one fsync per call, while preserving "acknowledged means durable".
- Make log reads proportional to the requested range: replace the filter-plus-sort over the whole entries map in `Log.fold/6` with direct index-range lookups, and avoid whole-log scans when deriving the last index/term.
- Reduce per-append bookkeeping: use atomic ETS counters for metrics, skip redundant metric-table initialization on the hot path, and avoid rebuilding peer context maps for every replicated batch.
- Make startup recovery linear in WAL records and entries, including snapshot and segment metadata loading.
- Add a repeatable benchmark suite that measures WAL append, log fold, apply, recovery, replication, and query paths, records results in `docs/`, and can compare against a stored baseline.
- No public API or Raft safety behavior changes. **BREAKING**: none.

## Capabilities

### New Capabilities

- `write-ahead-log`: durable append semantics, group commit, bounded buffering, exact and linear-time recovery, and truncation behavior.
- `observability`: atomic, non-blocking metric updates whose values stay accurate under concurrent servers.
- `performance-benchmarks`: a repeatable benchmark harness with documented baselines and a comparison mode for detecting regressions.

### Modified Capabilities

- `log-replication`: replication and commit application must read and apply only the entries in the requested batch, with work proportional to batch size rather than total log size.

## Impact

- `lib/infra/persistence/log_wal.ex`: buffer representation, flush/sync, recovery, truncate-from, read, metrics.
- `lib/infra/persistence/log.ex`: `fold/6`, last-index/term derivation, append and truncate paths.
- `lib/infra/observability/metrics.ex`: counter and histogram updates.
- `lib/domain/server/server.ex`, `lib/domain/server/rpc_handler.ex`, `lib/app/server_runtime.ex`: replication batch construction, commit application, per-message persistence bookkeeping.
- `lib/infra/persistence/log_segment_writer.ex` and `log_snapshot.ex`: startup recovery metadata.
- `scripts/` and `docs/`: new benchmark harness and recorded baselines.
- Tests: existing 861-test suite must stay green; new regression tests cover WAL group commit, recovery equivalence, and metric accuracy under concurrency.
