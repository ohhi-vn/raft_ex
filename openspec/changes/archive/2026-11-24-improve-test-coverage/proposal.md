# Proposal

## Why

RaftEx's test suite (130 tests) passes, but coverage is uneven: many modules sit at 0–35% and several core paths (election, RPC handling, server runtime, persistence snapshot/WAL) are largely untested. The migration to Clean Architecture just landed, so this is the right moment to lock in behavior with tests before further feature work. Low coverage hides regressions in the consensus algorithm and makes refactoring risky.

## What Changes

- Add unit and integration tests for the modules listed in the coverage report, prioritizing 0% and sub-40% modules.
- Pure domain modules (`RaftEx.Domain.*`) get fast, OTP-free unit tests exercising logic directly.
- Application/infrastructure modules (`RaftEx.App.*`, `RaftEx.Infra.*`) get targeted tests using temporary data dirs and existing test helpers; heavy OTP integration stays in the existing suite.
- No production behavior changes; only minimal testability refactors where a module cannot be exercised otherwise (e.g., exposing pure helpers, injecting dependencies). Any such refactor is called out per task.
- Coverage target: raise every listed module to at least 95% line coverage, with an overall total of at least 95%. Unreachable OTP glue may be exempted with recorded justification.

## Capabilities

### New Capabilities
(none)

### Modified Capabilities
(none)

This change sets `skip_specs: true` in `.openspec.yaml` (test-only, no behavior change).

## Impact

- Affected code: `test/` (new test files and additions to `test/raft_ex_test.exs`); possible small refactors in `lib/` for testability.
- APIs: none breaking; only additive test helpers if needed.
- Dependencies: none new.
- Systems: CI coverage (ExCoveralls) improves; `mix test` runtime increases modestly.