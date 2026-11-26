# Design

## Context

RaftEx is an Elixir port of RabbitMQ RA. The codebase already ships leader election, log replication, snapshots, cluster membership, consistent queries, leadership transfer, per-server WAL files with fsync, and a seshat-backed counter wrapper. The existing test suite (821+ tests, in-BEAM 3/5/7-server clusters) and the archived `complete-raft-consensus` change define the behavioural contract. This design does not alter that contract; it hardens verification, persistence invariants, WAL scalability, performance, and release hygiene around it.

Constraints shaping the approach:
- Keep the public API stable and backwards compatible; new surface is additive.
- Preserve the per-server WAL file layout as the default so existing data directories and recovery tests keep working.
- No new required runtime dependencies for library users; fault-injection/linearizability helpers stay in `:test` (or optional).
- Metrics must degrade gracefully when `:seshat` is unavailable (existing pattern in `Counters`).
- All work must keep `mix precommit` green.

## Goals / Non-Goals

**Goals:**
- Make correctness under failure *measured* rather than assumed (fault injection + linearizability).
- Make the index invariants explicit, asserted, and tested across crash/recovery.
- Provide a credible, opt-in path to a shared WAL for many consensus groups.
- Add pipelined/adaptive replication and backpressure with observable metrics.
- Make release metadata and feature status self-consistent and machine-checkable.

**Non-Goals:**
- Replacing the per-server WAL as default (investigation/prototype only).
- Changing Raft protocol semantics or the public API.
- Supporting multi-host distributed clusters in this change (in-BEAM multi-node only; multi-host remains future work).
- Building a full Jepsen deployment; an in-process history checker is sufficient for v1.

## Decisions

### D1: Fault-injection harness lives in test support, not production code
**Choice:** Implement partition/crash/delay/disk-failure primitives as test helpers under `test/support/` and `test/fixtures/`, driven by ExUnit tags/profiles. A `:chaos` mix profile runs them separately from the fast suite.
**Rationale:** Keeps production code free of test-only hooks; failure injection is exercised through real process supervision and real filesystem (tmp dirs), which matches how bugs actually manifest.
**Alternatives:** In-process "faulty transport" wrapper around `RaftEx.Infra.Network.Transport` — rejected because it tests the wrapper, not the real path, and diverges from production.

### D2: Linearizability via an in-process history checker
**Choice:** Record a history of `{invoke, ok, fail}` operations (consistent query / aux command / process command) with client ids and timestamps, then run a linearizability verifier (port of a small checker, or `porcupine`-style) over it. Store histories as artifacts on failure for replay.
**Rationale:** Avoids a Jepsen dependency and JVM; still catches the real classes of bugs (stale reads, lost updates). Jepsen remains a documented future option.
**Alternatives:** Full Jepsen cluster — deferred (multi-host, operational cost).

### D3: Index invariants as a single `Invariant` module + debug assertions
**Choice:** Add `RaftEx.Domain.Invariant` (pure functions) defining each invariant and a `check/2` that raises in `:test`/debug builds. Call sites: server state transitions (commit/apply/snapshot/restore) and recovery. Recovery reconstruction order becomes: load snapshot → replay WAL from `snapshot_index+1` → restore metadata (term/voted_for/last_applied/commit) → recompute `last_index` from log → assert all invariants.
**Rationale:** Centralises the contract so tests and production agree; assertions are cheap (integer compares) and can be compiled out in prod via `Application.compile_env`.
**Alternatives:** Scattered `assert` in each module — rejected (drift, inconsistent).

### D4: Shared WAL behind a behaviour + feature flag
**Choice:** Define `RaftEx.Infra.Persistence.Wal` behaviour with two implementations: `PerServerWal` (current, default) and `SharedWal` (prototype). `LogWal` becomes a facade selecting implementation from config (`:wal_mode`, default `:per_server`). Shared WAL batches writes across uids in one process/file with per-uid index ranges and a shared fsync; format documented separately. Migration: read old per-server files on first boot, rewrite into shared file, keep a backup.
**Rationale:** Preserves default behaviour and data; lets investigation proceed without destabilising users.
**Alternatives:** Big-bang rewrite — rejected (risk to recovery correctness).

### D5: Pipelined AppendEntries with bounded in-flight queue
**Choice:** Track in-flight AppendEntries per follower (`next_index`, `match_index`, pending requests with `ref`/`prev_index`). Allow up to `pipeline_depth` (config, default small) concurrent requests; handle acks out of order by advancing `match_index` only when contiguous. Adaptive batch size grows/shrinks based on ack latency and follower lag. Slow followers get a bounded queue; when full, leader skips sending and records `replication_backpressure` metric rather than blocking.
**Rationale:** Standard Raft pipelining; bounded queue prevents unbounded memory under a slow follower; metrics expose the condition.
**Alternatives:** Unbounded in-flight — rejected (OOM risk, the exact problem backpressure solves).

### D6: Metrics catalogue + central registration
**Choice:** Extend `Counters` with named fields for WAL latency (histogram-ish via min/max/sum/count), replication lag (per-follower), election duration, quorum size, backpressure events. Register per-server counters at server start (existing pattern). Document all metric names/labels in `docs/metrics.md`. Keep seshat optional.
**Rationale:** Reuses existing infra; avoids a hard dep.

### D7: Release hygiene automation
**Choice:** Add a `mix release.check` alias (or extend `precommit`) that verifies: `mix.exs` version == latest CHANGELOG heading version == git tag (if present) == docs `source_ref`. Add a feature-status table template in README driven by a small `docs/feature-status.md`. Add a release checklist doc. CI runs the check.
**Rationale:** Single source of truth for version; machine-checkable prevents drift.

## Risks / Trade-offs

- [Fault injection tests are slow/flaky] → Run under a separate `:chaos` profile; seed control; quarantine flaky tests with clear ownership rather than disabling.
- [Invariant assertions in prod could crash a node on a latent bug] → Compile-time flag `config :raft_ex, :invariant_checks, :debug | :test | :off`; default `:off` in prod, `:test` in test.
- [Shared WAL increases blast radius (one file, many groups)] → Prototype only, opt-in, per-uid index ranges, checksums per record, recovery tests before promotion.
- [Pipelining complexity / out-of-order ack bugs] → Extensive property-based + fault-injection tests; keep `pipeline_depth` default 1 (current behaviour) until validated.
- [Metrics overhead] → Counters are ETS/seshat; sample rather than per-RPC where hot; benchmark before/after.
- [Docs drift recurs] → Automate version check; feature-status table reviewed in PR template.

## Migration Plan

1. Land invariant module + assertions (no behaviour change) with tests.
2. Add fault-injection harness and linearizability checker; baseline existing behaviour.
3. Add metrics catalogue + counters (additive).
4. Prototype shared WAL behind flag; benchmark; document.
5. Implement pipelining + adaptive batching + backpressure behind config (default off).
6. Release hygiene: version check, feature-status matrix, checklist, CI.

Rollback: each step is independently revertible; shared WAL and pipelining are flag-gated; metrics are additive.

## Open Questions

- Whether to adopt a real property-based testing library (StreamData) or hand-rolled generators — deferrable; start hand-rolled, migrate if useful.
- Exact seshat field types for histograms (min/max/sum/count vs. multiple counters) — deferrable; decide during D6 implementation.