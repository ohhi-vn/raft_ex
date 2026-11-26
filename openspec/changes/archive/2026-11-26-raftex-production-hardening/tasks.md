# Tasks

## 1. Persistence invariants (Priority 2)

- [x] 1.1 Create `RaftEx.Domain.Invariant` module defining the index invariants; verify unit tests cover each violation
- [x] 1.2 Add `check/1`, `check!/1`, `check_recovery/1`, `check_recovery!/1` gated by `config :raft_ex, :invariant_checks`; verify via config toggle test
- [x] 1.3 Wire invariant checks into recovery via `check_recovery!/1`; verify existing 821+ tests still pass with checks enabled
- [x] 1.4 Make recovery deterministic and idempotent: verify by running recovery twice (recovery_idempotency_test.exs)
- [x] 1.5 Document the invariants and recovery ordering in `docs/persistence-invariants.md`

## 2. Fault-injection harness (Priority 1)

- [x] 2.1 Add `test/support/fault_injection.ex` helpers (crash, disk failure, delay, env override); verify a sample test exercises them
- [x] 2.2 Add `test/chaos/` directory with `:chaos`-tagged sample test; verify `mix test --only chaos` runs only chaos tests
- [x] 2.3 Add seed control (`CHAOS_SEED`) and artifact dumping helpers in FaultInjection; verify via unit usage
- [x] 2.4 Write chaos test for leader crash mid-replication (3-server); verify it passes

## 3. Linearizability checker (Priority 1)

- [x] 3.1 Add `test/support/linearizability.ex` recording operation history with invoke/complete
- [x] 3.2 Implement an in-process linearizability verifier; verify it detects a non-linearizable history in a unit test
- [x] 3.3 Add chaos test with mixed workload and partition/heal; verify it passes

## 4. Crash/recovery test matrix (Priorities 1+2)

- [x] 4.1 Matrix: full cluster restart recovery (committed state preserved); verify recovery succeeds (recovery_matrix_test.exs)
- [x] 4.2 Matrix: restart with corrupt WAL tail; verify graceful truncation to last good record; verify recovery succeeds (wal_corruption_test.exs)
- [x] 4.3 Matrix: snapshot + compaction + restart recovery; verify invariant checks pass (recovery_matrix_test.exs)

## 5. Metrics and observability (Priority 4)

- [x] 5.1 Extend `Counters` with new fields and convenience inc/observe/set_gauge delegating to ETS Metrics; verify compile
- [x] 5.2 Instrument WAL flush path with count + latency via `RaftEx.Infra.Observability.Metrics`; verify metrics update on append (metrics_test.exs)
- [x] 5.3 Instrument election and quorum paths with metrics; verify metrics update during a cluster run (Metrics.inc in Election/Server)
- [x] 5.4 Add `docs/metrics.md` catalogue; verify `mix docs` includes it
- [x] 5.5 Add `RaftEx.Observability` read API for current metrics; verify it compiles and runs

## 6. Pipelined AppendEntries (Priority 4)

- [x] 6.1 Per-follower in-flight tracking already implemented via next_index/match_index and make_pipelined_rpc_effects (max_pipeline_count)
- [x] 6.2 match_index advanced on successful AppendEntries reply; conflicting/late acks handled by RpcHandler
- [x] 6.3 Adaptive batch sizing implemented via effective_batch = min(batch_size, max_pipeline - in_flight)
- [x] 6.4 Pipelining gated behind max_pipeline_count (config); documented as pipeline_depth in README

## 7. Backpressure for slow followers (Priority 4)

- [x] 7.1 Bounded in-flight queue already implemented (in_flight >= max_pipeline skips send); records `:replication_backpressure` metric
- [x] 7.2 Add leader-side throttling (skip ticks with decaying backpressure_count); verify replication resumes when follower catches up
- [x] 7.3 Document performance config knobs in README; verify documented knobs exist in config

## 8. Shared WAL investigation and prototype (Priority 3)

- [x] 8.1 Define `RaftEx.Infra.Persistence.Wal` behaviour contract (documentation + module); verify it compiles
- [x] 8.2 Implement `SharedWal` prototype (single shared file, per-uid index ranges); verify append/read/recover round-trip tests
- [x] 8.3 Add migration path from per-server files to shared file (backup retained); verify migration test
- [x] 8.4 Add WAL benchmark script (1/10/50 uids) and `docs/wal-benchmark.md`; verify script runs
- [x] 8.5 Gate shared WAL behind `config :raft_ex, :wal_mode` (default `:per_server`); document experimental status in README

## 9. Documentation and release hygiene (Priority 5)

- [x] 9.1 Add feature-status matrix to README; verify every component row has a status
- [x] 9.2 Add `mix release.check` alias + script verifying version sync; verify alias exits 0 on clean tree
- [x] 9.3 Add release checklist document (`docs/release-checklist.md`)
- [x] 9.4 Add CI workflow running `mix precommit` + `mix release.check` (`.github/workflows/ci.yml`)

## 10. Integration verification

- [x] 10.1 Run `mix precommit` with invariant checks enabled and all new tests; verify 0 failures
- [x] 10.2 Run chaos profile; verify all pass (3 chaos tests)
- [x] 10.3 Generate docs (`mix docs`) and verify no warnings; feature-status and metrics pages render

## Workflow follow-up

- Archive the change after review requirements are satisfied.
- Verify the archived result.