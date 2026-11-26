# Proposal

## Why

RaftEx has reached feature completeness for a single-BEAM, single-system Raft cluster (leader election, log replication, snapshots, membership, queries, startup), but it is not yet credible as a long-term open-source or production-grade library. The five gaps that most undermine that credibility are: (1) no fault-injection or linearizability verification, so correctness under partitions/crashes is unmeasured; (2) no explicit, tested invariants tying WAL persisted index, log last index, commit index, last applied, and snapshot index together across crash/recovery; (3) a per-server WAL design that will not scale to many independent consensus groups; (4) no pipelining, adaptive batching, backpressure, or replication/WAL/election metrics; and (5) unsynchronized documentation and release metadata that do not distinguish implemented, tested, experimental, and production-ready features. This change addresses all five as a single hardening initiative so the project's public claims match its verified behaviour.

## What Changes

- **Distributed correctness verification (Priority 1)**
  - Add a fault-injection test harness (network partitions, node crashes, delayed RPCs, disk failures, recovery) runnable against in-BEAM multi-node clusters.
  - Integrate a history-based linearizability checker (Jepsen-style or an Elixir equivalent) over the consistent-query/aux-command API, recording operations and verifying linearizability post-run.
  - Add a reproducible "chaos" test profile and seed control so failures are replayable.
- **Persistence correctness (Priority 2)**
  - Define and document the index invariants: `snapshot_index <= wal_persisted_index <= log_last_index`, `commit_index <= log_last_index`, `last_applied <= commit_index`, and recovery reconstruction of each from durable state (WAL + snapshot + metadata).
  - Add invariant assertions (debug/test mode) at every state transition that mutates these indices, plus a dedicated crash/recovery test matrix.
  - Make recovery deterministic and idempotent: re-running recovery yields identical state.
- **WAL architecture (Priority 3)**
  - Investigate and prototype a shared, multi-server WAL with cross-server batching (similar to RA's shared log), behind a feature flag, preserving the existing per-server file layout as the default.
  - Define the migration/compatibility contract between per-server and shared WAL formats.
  - Benchmark throughput/latency of both designs under many consensus groups.
- **Performance and observability (Priority 4)**
  - Implement pipelined AppendEntries (in-flight request tracking, out-of-order ack handling) and adaptive batching (batch size tuned by follower lag and RTT).
  - Add backpressure for slow followers (bounded in-flight queue, leader-side throttling, observable drop/queue metrics).
  - Add WAL latency, replication lag, election, and quorum metrics via the existing seshat counter wrapper, plus a documented metric catalogue.
- **Documentation and release management (Priority 5)**
  - Synchronize Hex documentation, README, CHANGELOG, and GitHub release tags/versions (`mix.exs` version, `docs` source_ref, CHANGELOG headings).
  - Introduce a feature-status matrix (implemented / tested / experimental / production-ready) in README and Hex docs, and keep it updated per release.
  - Add a release checklist and automate version bump + changelog validation in CI/precommit.

## Capabilities

This is a hardening, testing, performance, and documentation initiative. No runtime externally-observable behaviour is added or changed at the Raft protocol level; the guarantees being made explicit (index invariants, metrics surface aside) already hold implicitly. Therefore no spec-level deltas are produced and `skip_specs: true` is set in `.openspec.yaml`. The authoritative behaviour specs (leader-election, log-replication, snapshots, cluster-membership, raft-queries, raft-startup) remain the contract; this change strengthens verification, performance, and developer experience against them.

## Impact

- Affected code: test suite and test support (`test/`, `test/support/`), `RaftEx.Infra.Persistence.LogWal`, `RaftEx.Infra.Persistence.Log`, `RaftEx.Infra.Observability.*`, `RaftEx.Domain.Server.*` (invariant hooks), `RaftEx.App.ServerRuntime`, README, CHANGELOG, `mix.exs`, docs config.
- APIs: no breaking public API changes intended; metrics are additive. Shared WAL is opt-in behind a flag during investigation.
- Storage: per-server WAL layout remains default; shared WAL is experimental and gated.
- Tests: large additions (fault injection, linearizability, crash/recovery matrix, benchmarks); existing 821+ tests must keep passing.
- Dependencies: may add a fault-injection/linearizability helper (Elixir) and CI tooling; keep optional to avoid forcing deps on library users.
- Documentation: README status matrix, Hex docs, CHANGELOG, release checklist.
EOF
echo done