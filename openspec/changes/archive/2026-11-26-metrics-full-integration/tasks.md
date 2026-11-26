# Tasks

## 1. Catalogue

- [x] 1.1 Create `RaftEx.Infra.Observability.MetricNames` (or extend Counters) with all metric atoms + kinds.
- [x] 1.2 Update `docs/metrics.md` to full catalogue (name, kind, labels, units, source).
- [x] 1.3 Extend and make public `counter_fields/0` to match catalogue (all metric atoms).

## 2. Wiring

- [x] 2.1 Election: `elections_started` already wired; record `elections_won`, `term_changed`, `votes_granted`, `elections_lost`.
- [x] 2.2 Replication: `replication_backpressure` already wired; record `append_entries_sent`, `append_entries_acked`, per-follower `replication_lag_ms` gauge, `commit_index` gauge.
- [x] 2.3 Quorum: `quorum_reached` already wired; add `commit_advanced`.
- [x] 2.4 Snapshots: record chunks sent/received/installed/failed and transfer duration.
- [x] 2.5 Leadership transfer: record transfers initiated/completed/failed.
- [x] 2.6 WAL: `wal_flushes` and `wal_flush_latency_ms` already wired; add bytes written, segment writes, fsync count.
- [x] 2.7 Server init: call `Counters.new/3` with `Counters.counter_fields()` (in `server_sup_supervisor`).

## 3. Observability API

- [x] 3.1 Add `RaftEx.Observability.cluster_metrics/1` and `server_metrics/1` (if missing).
- [x] 3.2 Ensure `RaftEx.Observability.reset/0` clears ETS for tests.
- [x] 3.3 Fix `Metrics.get/1` (and thus `Observability.metric/1`) to return gauge and histogram values, not only counters.
- [x] 3.4 Add per-server counter update helpers `Counters.inc/3`, `observe/3`, `set_gauge/3` (server_id, name, value).

## 4. Tests and docs

- [x] 4.1 Add `test/infra/observability/metrics_test.exs` for catalogue presence/monotonicity.
- [ ] 4.2 Add metric-coverage check to `mix precommit` or CI (best-effort).
- [x] 4.3 Update README feature matrix metrics row.
- [x] 4.4 Run `mix precommit`.
