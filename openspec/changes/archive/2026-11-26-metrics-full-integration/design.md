# Design

## Context

Two stores coexist: `Metrics` (ETS, always available, flat names) and `Counters` (seshat, optional, per-server grouped). Wiring is ad hoc: WAL uses `Metrics` directly; server/election use `Metrics` directly with flat names. `Counters` exposes `inc/2`, `observe/2`, `set_gauge/2` that delegate to `Metrics`, so calling `Counters.inc(:elections_started)` already lands in ETS. But per-server seshat counters (`Counters.new/2`, `fetch/1`) are separate and rarely used. The field list in `counter_fields/0` is short and not aligned with the ETS names used.

## Goals / Non-Goals

**Goals:**
- Single source of truth for metric names (a catalogue module/constant).
- Every metric recorded in both stores consistently.
- Per-server counters created at server init with full field spec.
- Documented catalogue with kinds, labels, units.

**Non-Goals:**
- Replacing ETS with seshat or vice versa.
- Exporting to Prometheus/OTLP (future).
- Changing metric semantics of existing counters.

## Decisions

- **Catalogue**: add `RaftEx.Infra.Observability.MetricNames` (or extend `Counters`) listing all metric atoms and their kind. Both stores reference it; docs generated from it.
- **Dual write**: `Counters.inc/observe/set_gauge` delegate to `Metrics` (ETS), which is the guaranteed store. Per-server seshat groups are optional; updating them requires server context, so add `Counters.inc/3`, `observe/3`, `set_gauge/3` (server_id, name, value) that write ETS globally and the per-server seshat group. No duplicate ETS writes.
- **Per-server init**: in `server_sup_supervisor.start_server_rpc/3` (and the restart path), call `Counters.new(server_id, Counters.counter_fields())`. Make `counter_fields/0` public so callers can pass the full field spec. Per-server grouping is provided by the seshat group `{:ra, server_id}` created by `Counters.new/3`; seshat in this version has no arbitrary labels, so the earlier "labels `%{cluster, node}`" idea is dropped in favour of the server id as the group key. ETS stays flat (name only).
- **New metrics**: add atoms for election_won, term_changed, votes_granted, append_entries_sent/acked, replication_lag_ms (gauge per follower), commit_index (gauge), snapshot_chunks_sent/received/installed/failed, leadership_transfers, wal_bytes_written, etc. Use `Metrics.set/2` for gauges.
- **Labels**: ETS store stays flat (name only). seshat does not support arbitrary labels in this version; per-server grouping is achieved via the server id group key. Document this limitation.
- **Testing**: add `test/infra/observability/metrics_test.exs` asserting each catalogue metric can be recorded and read; add a compile-time/CI check that all `Metrics.*` calls use catalogue names (optional, best-effort via credo custom check or grep).
- **Observability API**: `RaftEx.Observability.metrics/0`, `metric/1`, `seshat_overview/0`, `reset/0` already exist; `server_metrics/1` and `cluster_metrics/1` are to be added. Note: `Metrics.get/1` currently only returns counter values; it must also read gauges and histograms so `metric/1` can observe gauges such as `replication_lag_ms` and `commit_index`.

## Risks / Trade-offs

- Metric name proliferation; mitigate via catalogue and review.
- seshat optional; ETS is the guaranteed store, so tests assert ETS.
- Per-server ETS keys would need labels; keep flat global counters + per-server seshat grouping to avoid ETS key explosion.
