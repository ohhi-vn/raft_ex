# Proposal

## Why

The `:seshat` counter wrapper (`Counters`) and the always-available ETS store (`Metrics`) exist, and a handful of counters are wired (`wal_flushes`, `wal_flush_latency_ms`, `elections_started`, `quorum_reached`, `replication_backpressure`). But the catalogue in `docs/metrics.md` is incomplete: election wins, term changes, replication lag, commit latency, snapshot transfers, append-entries round trips, and per-server WAL metrics are missing or inconsistent. As a result, operators cannot observe replication health, election churn, or backpressure, and the feature matrix marks Metrics as only partially implemented/tested.

## What Changes

- Define a complete metric catalogue (names, kinds, labels, units) in `docs/metrics.md`, covering WAL, replication, election, quorum, snapshots, and leadership transfer.
- Wire metrics into the remaining code paths: election won/lost, term increments, vote grants, append-entries send/ack latency, replication lag (per follower), commit advancement, snapshot chunks sent/received/installed/failed, leadership transfer.
- Unify the two stores: metrics recorded through the global `Counters` helpers already land in ETS; per-server seshat groups are updated via new server-context helpers so ETS remains the guaranteed store; document the mapping.
- Create per-server seshat counter groups at server init via `Counters.new/3` with the full field spec (make `counter_fields/0` public). seshat has no arbitrary labels in this version, so grouping is by server id.
- Add `RaftEx.Observability` helpers for reading per-server and cluster-wide metrics, plus a reset for tests.
- Add tests for metric presence/monotonicity and a metric-coverage check in CI/precommit.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

(none)

No spec-level behaviour changes: metrics are observability of existing behaviour. `skip_specs: true` is set in `.openspec.yaml`.

## Impact

- Affected code: `lib/infra/observability/*`, `lib/domain/server/*`, `lib/infra/persistence/log_wal.ex`, `lib/app/server_runtime.ex`, `lib/observability.ex`, `lib/raft_ex.ex`, `docs/metrics.md`.
- APIs: additive (`RaftEx.Observability` helpers); no breaking changes.
- Dependencies: `:seshat` remains optional.
- Tests: metric presence/monotonicity tests; existing suite unaffected.
