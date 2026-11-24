# Tasks

## 1. Scaffold and dependency rules

- [x] 1.1 Create layer directories `lib/domain`, `lib/app`, `lib/infra`, `lib/interface` and verify `find lib -type d` shows them.
- [x] 1.2 Add a compile-time layer guard (e.g. `lib/layer_guard.ex` or credo custom check) that fails if `RaftEx.Domain.*` references `RaftEx.App.*`/`RaftEx.Infra.*`/`RaftEx.Interface.*`, and verify `mix compile` fails on a deliberate violation then passes after revert.
- [x] 1.3 Update `mix.exs` `application/0` to reference `RaftEx.OTPApplication` (placeholder) and verify `mix compile` still resolves the callback module name (will be wired in group 5).

## 2. Domain layer - types and pure utilities

- [x] 2.1 Move `RaftEx.Types` to `RaftEx.Domain.Types` (file `lib/domain/types.ex`), update all internal aliases, and verify `mix compile` succeeds.
- [x] 2.2 Split `RaftEx.Lib` pure helpers into `RaftEx.Domain.Uid` (UID generation) and `RaftEx.Domain.Codec` (to_list/to_binary/to_atom and base64 helpers), keeping file helpers out, and verify unit-level calls in tests resolve.
- [x] 2.3 Move `RaftEx.Range`, `RaftEx.Seq`, `RaftEx.LeakyIntegrator`, `RaftEx.LruCache` to `RaftEx.Domain.*`, and verify `mix compile` succeeds.
- [x] 2.4 Fix naming: move `lib/machine/machine_simpile.ex` to `lib/domain/machine/simple.ex` as `RaftEx.Domain.Machine.Simple`, and verify the file no longer named `simpile`.
- [x] 2.5 Move `RaftEx.Machine` behaviour to `RaftEx.Domain.Machine`, add a deprecated alias `RaftEx.Machine = RaftEx.Domain.Machine`, and verify existing `@behaviour RaftEx.Machine` callers still compile.
- [x] 2.6 Move domain submodules `RaftEx.Server.Config`, `Server.Cluster`, `Server.Effects`, `Server.Election`, `Server.Rpc.RpcHandler` to `RaftEx.Domain.Server.*`, and verify `mix compile`.
- [x] 2.7 Move `RaftEx.Server` core logic to `RaftEx.Domain.Server`, ensuring no `Logger`/`:ets`/`File`/`Node`/`:rpc` calls remain (grep check), and verify `mix compile`.
- [x] 2.8 Update doctests and `RaftEx.Domain.*` `@moduledoc` references; run `mix test --only doctest` (or relevant) for touched modules.

## 3. Application ports and use cases

- [x] 3.1 Define port behaviours in `RaftEx.App.Ports` (`Persistence`, `Transport`, `Clock`, `ProcessSpawner`) with callbacks matching current infra usage, and verify each compiles.
- [x] 3.2 Refactor `RaftEx.Domain.Server` to depend on ports instead of direct infra calls (inject adapters via state/config), and verify `mix compile`.
- [x] 3.3 Move `RaftEx.ServerProc` to `RaftEx.App.ServerRuntime` (gen_statem host), wiring ports to infra adapters, and verify server lifecycle tests pass.
- [x] 3.4 Move `RaftEx.System` to `RaftEx.App.System` and `RaftEx.SystemRecover` to `RaftEx.App.Recovery`, updating port wiring, and verify `mix compile`.
- [x] 3.5 Move `RaftEx.Cluster` to `RaftEx.App.Cluster`, and verify cluster management tests pass.
- [x] 3.6 Add deprecated aliases for moved App modules where external callers may reference them, and verify compile.

## 4. Infrastructure layer - persistence

- [x] 4.1 Move `RaftEx.File` helpers to `RaftEx.Infra.Filesystem`, and verify callers compile.
- [x] 4.2 Move `RaftEx.LogEts`, `LogMeta`, `LogWal`, `LogWalSupervisor` to `RaftEx.Infra.Persistence.*`, and verify `mix compile`.
- [x] 4.3 Move `RaftEx.LogSegmentWriter`, `LogSnapshot`, `LogSnapshotState`, `LogPreInit` to `RaftEx.Infra.Persistence.*`, and verify `mix compile`.
- [x] 4.4 Move `RaftEx.Log` orchestration to `RaftEx.Infra.Persistence.Log`, implementing the `Persistence` port, and verify log tests pass.
- [x] 4.5 Move `RaftEx.Snapshot` to `RaftEx.Infra.Persistence.Snapshot`, and verify snapshot tests pass.
- [x] 4.6 Move `RaftEx.Directory` to `RaftEx.Infra.Registry.Directory`, and verify directory tests pass.

## 5. Infrastructure layer - network, observability, supervisors, config

- [x] 5.1 Move `RaftEx.Network` to `RaftEx.Infra.Network.Transport`, `NodeMonitor` to `RaftEx.Infra.Network.NodeMonitor`, `Distribution` to `RaftEx.Infra.Network.Distribution`, implementing the `Transport` port, and verify `mix compile`.
- [x] 5.2 Move `RaftEx.Counters`, `MetricsEts`, `Monitors` to `RaftEx.Infra.Observability.*` (singular `Monitor`), and verify `mix compile`.
- [x] 5.3 Move supervisors (`RaftEx.Supervisor`, `ServerSupervisor`, `ServerSupSupervisor`, `SystemSupervisor`, `SystemsSupervisor`) to `RaftEx.Infra.Supervisor.*`, and verify `mix compile`.
- [x] 5.4 Move `RaftEx.Env` to `RaftEx.Infra.Config.Env`, and verify `mix compile`.
- [x] 5.5 Rename OTP application callback module `RaftEx.Application` to `RaftEx.OTPApplication` (file `lib/infra/otp_application.ex` or `lib/otp_application.ex`), update `mix.exs`, and verify `mix compile` + `Application.ensure_all_started(:raft_ex)` in tests.

## 6. Interface layer - facade and mix task

- [x] 6.1 Update `RaftEx` facade to delegate to new module paths; keep public function signatures stable, and verify `mix compile`.
- [x] 6.2 Update `Mix.Tasks.RaftEx.Cluster` references, and verify `mix raft_ex.cluster` (or task listing) works.
- [x] 6.3 Add deprecated aliases for commonly referenced old modules (`RaftEx.Types`, `RaftEx.Server`, `RaftEx.Log`) pointing to new locations, and verify compile.

## 7. Tests and fixtures

- [x] 7.1 Update `test/raft_ex_test.exs` module references to new paths, and verify `mix test` passes for unit suites.
- [x] 7.2 Update `test/multi_node_test.exs` references, and verify multi-node tests pass (when distribution available).
- [x] 7.3 Update `test/fixtures` (if any) module references, and verify `mix test` compiles fixtures.
- [x] 7.4 Remove deprecated aliases once tests no longer use old names (optional cleanup), and verify `mix compile` with aliases removed.

## 8. Quality, docs, and verification

- [x] 8.1 Run `mix format` and verify `mix format --check-formatted` passes.
- [x] 8.2 Run `mix credo --strict` and fix all reported issues; verify exit code 0.
- [x] 8.3 Run `mix dialyzer` and resolve warnings or update `.dialyzer_ignore.exs` with justification; verify exit code 0.
- [x] 8.4 Run full `mix test` and verify all tests pass (107+ baseline).
- [x] 8.5 Update README module references and CHANGELOG entry for the migration; verify links/examples still reference valid modules.
- [x] 8.6 Final grep gate: ensure no references to removed old module names remain in `lib/` and `test/` (except deprecated aliases), and verify command returns empty.

## Workflow follow-up

- Archive the change after review requirements are satisfied.
- Verify the archived result.