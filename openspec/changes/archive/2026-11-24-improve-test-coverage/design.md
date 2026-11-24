# Design

## Context

The Clean Architecture migration reorganized modules into `RaftEx.Domain.*` (pure logic), `RaftEx.App.*` (orchestration), and `RaftEx.Infra.*` (IO/OTP). Existing tests live in three flat files (`test/raft_ex_test.exs`, `test/multi_node_test.exs`, `test/plug_pipeline_test.exs`) and rely on a per-test temp data directory created in `RaftExTest`'s `setup`. Coverage is measured with ExCoveralls (`mix coveralls`). See proposal.md for motivation and the module list.

## Goals / Non-Goals

**Goals:**
- Organize new tests by layer so they mirror `lib/` structure and are easy to find.
- Cover the listed modules with fast, deterministic tests, prioritizing pure domain logic.
- Establish a reusable support module for temp dirs and fixtures to avoid duplication.
- Make coverage measurable and trackable per module via ExCoveralls.

**Non-Goals:**
- Changing Raft behavior or public APIs.
- Chasing 100% coverage; unreachable OTP error paths and defensive clauses may stay uncovered.
- Adding new test dependencies.
- Rewriting the existing passing tests wholesale.

## Decisions

- **Test layout by layer**: `test/domain/`, `test/app/`, `test/infra/` with one file per module (e.g. `test/domain/range_test.exs`). Rationale: matches the new `lib/` layout from the migration; keeps pure tests isolated from OTP-heavy ones. Alternative: append everything to `raft_ex_test.exs` — rejected because that file is already 1,730 lines and mixes concerns.
- **Pure-first**: Domain modules (`Range`, `Seq`, `LruCache`, `LeakyIntegrator`, `Machine`, `Pipeline`, `Plug`, `Conn`, `Utils`, `Types`, `Config`, `Effects`, `Cluster`) are tested by calling functions directly with no processes started. These use `async: true`. Alternative: integration-only tests — rejected because they are slow and hide which function failed.
- **Stateful/OTP modules**: App and Infra modules are tested with explicit setup/teardown. Reuse a `RaftEx.TestSupport` helper for temp dirs. Modules using ETS, named processes, or global state (`Monitor`, `Counters`, `MetricsEts`, `Directory`, `Leaderboard`, `MachineEts`, supervisors) use `async: false` to avoid cross-test interference. Rationale: correctness over parallelism.
- **Testability refactors only when necessary**: if a function is inseparable from IO/OTP, prefer extracting or exposing the pure computation rather than mocking internals. Allowed changes are minimal and additive (no behavior change) and are listed as discrete tasks.
- **Incremental verification**: after each layer's tests, run `mix test` and `mix coveralls` for the touched modules; keep changes only when green.
- **Privileged 0% modules first** (e.g. `ClusterSetup`, `RpcHandler`, `NodeDiscovery`, `Snapshot`, `Types`, `Worker`, `Mix.Tasks.RaftEx.Cluster`, `LogSnapshot`) to get the largest coverage gain, then low-coverage modules.

## Risks / Trade-offs

- [Timing-sensitive election/RPC tests become flaky] → Prefer state-transition tests on `Election` with injected context over wall-clock timeouts; keep any timing test in `async: false` with generous margins.
- [Tests leak ETS tables, named processes, or files] → Every setup registers `on_exit` cleanup; use unique names via `System.unique_integer/1`.
- [Refactors for testability change behavior] → Keep refactors additive (new helper functions), never alter existing call paths; verify full suite after each.
- [Large OTP modules (Server, ServerRuntime, Log) remain hard to cover] → Accept partial coverage; focus on their pure/edge paths and rely on existing integration tests for the happy path.
- [Longer CI runtime] → Keep pure tests `async: true`; integration additions stay modest.

## Migration Plan

No deployment migration. Implementation proceeds layer by layer (domain → infra → app → mix task), each step ending with green `mix test` and improved `mix coveralls`. Rollback is reverting the added test files.

## Open Questions

- None. Coverage target is a uniform 95% line coverage per listed module and 95% overall; any module that proves to be mostly unreachable OTP glue is exempted with a recorded justification in tasks.md.
- Follow-up push to 95% uses single-node integration tests that start a real server and exercise the facade, runtime callbacks, and effect-application branches through the public API.
