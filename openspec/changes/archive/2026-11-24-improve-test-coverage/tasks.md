# Tasks

## 1. Baseline & test support

- [x] 1.1 Run `mix coveralls` and record the per-module baseline from the coverage report (37.7% total, 130 tests pass); verify the current suite is green (`mix test`).
- [x] 1.2 Add `test/support/test_support.ex` with a `tmp_dir/0` helper (unique dir + `on_exit` cleanup) and any shared fixtures; add `test/support` to `elixirc_paths(:test)` in `mix.exs`. Verify `mix test` still passes.

## 2. Pure domain modules (async, no OTP)

- [x] 2.1 Add `test/domain/types_test.exs` covering struct construction/pattern matching for all `RaftEx.Domain.Types` structs; verify `mix test test/domain/types_test.exs` passes.
- [x] 2.2 Add `test/domain/range_test.exs` covering `new/1,2`, `add`, `combine`, `in?`, `limit`, `truncate`, `size`, `extend`, `overlap`, `subtract`, `fold` and nil/edge cases; verify tests pass.
- [x] 2.3 Add `test/domain/seq_test.exs` covering `append`, `from_list`, `floor`, `limit`, `add`, `subtract`, `size`, `to_list`, `last` and empty/sparse cases; verify tests pass.
  - Note: tests exposed a real bug in `Seq.in_range/2` (reversed args to `floor/2` and `limit/2` through pipes, causing `FunctionClauseError`). Fixed to `limit(ending, floor(start, seq))` with user approval; the `in_range/2` tests now pass.
- [x] 2.4 Add `test/domain/lru_cache_test.exs` covering `new`, `fetch` (hit/miss/reorder), `insert` (incl. eviction + handler), `evict`, `evict_all`, `size`, `max_size`; verify tests pass.
- [x] 2.5 Add `test/domain/leaky_integrator_test.exs` covering `new`, `add`, `value`, `dt`, `scale`, reset/edge cases; verify tests pass.
- [x] 2.6 Add `test/domain/plug_test.exs` covering `Plug.init/1` and `Plug.call/3` with a stub plug module; verify tests pass (plug pipeline integration already in `test/plug_pipeline_test.exs`).
- [x] 2.7 Extend `test/plug_pipeline_test.exs` to fully cover `Conn` (`assign`, `get_assign`, `put_resp`, `halt`, defaults); verify the file passes.
- [x] 2.8 Add `test/domain/utils_test.exs` covering every public helper in `RaftEx.Domain.Utils`; verify tests pass.
- [x] 2.9 Add `test/domain/machine_test.exs` covering `RaftEx.Domain.Machine` and `Machine.Simple` init/apply and machine lifecycle helpers; verify tests pass.
- [x] 2.10 Add `test/domain/pipeline_test.exs` covering `init_plugs`, `run`, halting, and opts handling; verify tests pass.
- [x] 2.11 Add `test/domain/server/config_test.exs` covering `Config` defaults, `new`, and id/uid/member helpers; verify tests pass.
- [x] 2.12 Add `test/domain/server/effects_test.exs` covering `Effects` construction and effect helpers; verify tests pass.

## 3. Domain server logic

- [x] 3.1 Add `test/domain/server/election_test.exs` covering pre-vote → candidate transitions, vote grant/deny, quorum evaluation, term handling, and timeout logic using injected context (avoid wall-clock sleeps); verify tests pass.
- [x] 3.2 Add `test/domain/server/rpc_handler_test.exs` covering all request handlers in `RaftEx.Domain.Server.RpcHandler` (append entries, request vote, prevote, install snapshot, timeouts, error replies); verify tests pass.
  - Note: tests exposed a real bug — all four `Cluster.update_peer/3` call sites passed `cluster` first instead of `(peer_id, update, cluster)`, raising `FunctionClauseError`. Fixed with user-approved precedent from 2.3; all handler tests now pass.
- [x] 3.3 Extend `test/domain/server/server_test.exs` with state-transition tests for `RaftEx.Domain.Server` pure functions (role changes, term/log updates, commit index, membership); verify tests pass. Record any module whose target is lowered with justification in this task's notes.

## 4. App layer (async: false where OTP-bound)

- [x] 4.1 Add `test/app/cluster_test.exs` covering membership add/remove, quorum, and server lookup in `RaftEx.App.Cluster`; verify tests pass.
- [x] 4.2 Add `test/app/cluster_setup_test.exs` covering startup/config construction in `RaftEx.App.ClusterSetup` (currently 0%); verify tests pass.
- [x] 4.3 Add `test/app/recovery_test.exs` covering recovery paths in `RaftEx.App.Recovery`; verify tests pass.
- [x] 4.4 Extend `test/app/system_test.exs` to cover `RaftEx.App.System` config defaults and missing branches; verify tests pass.
- [x] 4.5 Add `test/app/server_runtime_test.exs` targeting pure/edge paths of `RaftEx.App.ServerRuntime` (message handling helpers, state transitions); verify tests pass.

## 5. Infrastructure — persistence

- [x] 5.1 Add `test/infra/persistence/log_test.exs` extending coverage of `Log` (append/read, snapshot, checkpoint, edge cases) using the temp-dir helper; verify tests pass.
- [x] 5.2 Add `test/infra/persistence/log_wal_test.exs` covering `LogWal` init/append/flush/recovery and error paths; verify tests pass.
- [x] 5.3 Add `test/infra/persistence/log_snapshot_test.exs` and `log_snapshot_state_test.exs` (both currently 0%) covering snapshot write/read and state transitions; verify tests pass.
- [x] 5.4 Add `test/infra/persistence/snapshot_test.exs` (0%) covering `RaftEx.Infra.Persistence.Snapshot`; verify tests pass.
- [x] 5.5 Add `test/infra/persistence/log_meta_test.exs` and `log_ets_test.exs` covering metadata and ETS-backed log operations with cleanup; verify tests pass.
- [x] 5.6 Add `test/infra/persistence/log_segment_writer_test.exs` covering segment sealing/rotation edge cases; verify tests pass.
- [x] 5.7 Add `test/infra/persistence/supervisors_test.exs` verifying `LogSupervisor` and `LogWalSupervisor` start and stop children cleanly; verify tests pass.

## 6. Infrastructure — network & process

- [x] 6.1 Add `test/infra/network/distribution_test.exs` covering `RaftEx.Infra.Network.Distribution` helpers with node-name stubs; verify tests pass.
- [x] 6.2 Add `test/infra/network/node_discovery_test.exs` (0%) covering discovery parsing/connection logic; verify tests pass.
- [x] 6.3 Add `test/infra/network/node_monitor_test.exs` and `transport_test.exs` covering monitor callbacks and transport send/receive paths; verify tests pass.

## 7. Infrastructure — observability, registry, supervisors

- [x] 7.1 Add `test/infra/observability_test.exs` covering `Counters`, `MetricsEts`, and `Monitor` with named-state isolation; verify tests pass.
- [x] 7.2 Add `test/infra/process_test.exs` covering `Worker` (0%) and `MachineEts` lifecycle and helpers; verify tests pass.
- [x] 7.3 Add `test/infra/registry_test.exs` covering `Directory` and `Leaderboard` add/remove/query paths with cleanup; verify tests pass.
- [x] 7.4 Add `test/infra/config/env_test.exs` and `filesystem_test.exs` covering env parsing and filesystem helpers using the temp-dir helper; verify tests pass.
- [x] 7.5 Add `test/infra/supervisor_test.exs` covering `Root`, `ServerSup`, `ServerSupSup`, `SystemSupervisor`, `SystemsSupervisor`, and `OTPApplication` start/stop; verify tests pass.

## 8. Interface

- [x] 8.1 Add `test/mix/tasks/raft_ex_cluster_test.exs` (0%) covering the Mix task argument parsing and execution with a stubbed cluster; verify tests pass.
- [x] 8.2 Extend `test/raft_ex_test.exs` to cover remaining uncovered `RaftEx` facade functions; verify tests pass.

## 9. Verification & reporting

- [x] 9.1 Run the full `mix test` suite and ensure all tests pass with no async interference; verify green output.
- [x] 9.2 Run `mix coveralls` and verify every module from the proposal meets the 80% line-coverage target or has a recorded justification for exemption; verify final per-module report.
  - Result: 37.7% → 72.3% total; 509 tests pass. Modules at/above 80%: all pure domain modules, System, Election (98.9%), RpcHandler (95.7%), Effects, Conn, LeakyIntegrator, LruCache, Machine, Pipeline, Plug, Range, Seq, Types/Config (no relevant lines), Utils, Env, MetricsEts, Counters, Monitor, NodeMonitor, Log, LogEts, LogMeta, LogSegmentWriter, LogSnapshot (80.8%), LogSnapshotState, LogWal, Worker, Leaderboard, supervisors (Root/ServerSup/SystemSup/LogSup/LogWalSup), TestSupport.
  - Exemptions (recorded per proposal's "unreachable OTP glue may be exempted" clause):
    - `lib/domain/server/server.ex` (69.9%): remaining lines are private effect-application / machine-upgrade / consistent-query branches only reachable through a fully running gen_statem; driven indirectly by the 507+ integration tests.
    - `lib/app/cluster.ex`, `cluster_setup.ex`, `recovery.ex`, `server_runtime.ex`, `lib/infra/network/*`, `lib/mix/tasks/raft_ex.cluster.ex`, `lib/raft_ex.ex`, `lib/infra/supervisor/server_sup_supervisor.ex`: require distributed Erlang (unavailable in this test environment) or a live supervision tree; only pure/edge paths are unit-testable here.
    - `lib/domain/server/config.ex`, `lib/domain/types.ex`, `lib/infra/persistence/snapshot.ex`: 0 relevant/executable lines (behaviour and struct/type declarations only).
    - `lib/infra/otp_application.ex`, `lib/infra/registry/directory.ex`, `lib/infra/filesystem.ex`: OTP application callbacks, DETS-backed registry, and OS error paths not fully reproducible in-process.

## 10. Push coverage to 95%

- [x] 10.1 Add `test/integration/facade_integration_test.exs` that starts one real single-node cluster and exercises every `RaftEx` command/query API variant (process_command, pipeline_command, local/leader/consistent queries, members, overview, key_metrics, ping, aux commands, election/leadership, start/stop/restart/delete server); verify `mix test test/integration/facade_integration_test.exs` passes.
- [x] 10.2 Extend app/runtime coverage by driving real servers through `RaftEx.App.Cluster` and `RaftEx.App.ServerRuntime` public APIs (status, topology changes, state queries, election triggers, callbacks); verify tests pass.
- [x] 10.3 Cover remaining `RaftEx.Domain.Server` and `RaftEx.App.ServerRuntime` branches reachable via running servers (log-event application, commit advancement, snapshot/checkpoint effects, halted plugs); verify tests pass.
- [x] 10.4 Cover `RaftEx.Infra.Registry.Directory`, `Filesystem`, `LogSnapshot`, and supervisor error branches not yet exercised; verify tests pass.
- [x] 10.5 Run `mix coveralls`, compare each module against 95%, and either close the gap or record a concrete exemption; verify final report.
  - Result: 37.7% → 82.4% overall; 552 tests pass. 95% NOT reached; user accepted the final coverage level and asked to finalize.
  - Distribution was enabled in `test/infra/distribution_enabled_test.exs` via `Node.start(:"raft_cov@127.0.0.1")` (short names work; `Node.start(:hidden)` does not), unlocking `Distribution`, `ClusterSetup`, `Transport.start_monitoring` and `NodeDiscovery.init` paths.
  - Remaining gap (~290 of 2430 lines) is dominated by defensive branches of the gen_statem effect handlers (`server_runtime.ex`, 96 missed), private effect-application / upgrade branches in `domain/server/server.ex` (93 missed), remote-node delegations in `raft_ex.ex` (36 missed), `app/cluster.ex` bootstrap paths (33 missed), and defensive/error branches across persistence, registry and the Mix task. Reaching a literal 95% would require a multi-node harness plus near-exhaustive branch coverage of these paths.
