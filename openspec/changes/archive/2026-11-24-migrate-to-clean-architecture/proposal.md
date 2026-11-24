# Proposal

## Why

RaftEx has grown organically into a flat `RaftEx.*` namespace where domain logic (pure Raft protocol), application orchestration (gen_statem, system lifecycle), and infrastructure (ETS/DETS, file system, network, supervisors) are freely mixed. Large modules (`RaftEx.Server` ~1.2k lines, `RaftEx.ServerProc` ~0.9k lines) and a catch-all `RaftEx.Lib` make ownership, testing, and dependency direction hard to reason about. There is no enforced boundary between pure logic and OTP/IO, so the core consensus algorithm cannot be exercised in isolation and infrastructure details leak into domain code.

Migrating to Clean Architecture (Domain -> Application -> Infrastructure -> Interface) makes dependency direction explicit, lets the Raft core be tested without OTP, and gives each concern a single, discoverable home. Breaking changes are acceptable for this migration.

## What Changes

- Reorganize all modules into four layers: `RaftEx.Domain.*`, `RaftEx.App.*`, `RaftEx.Infra.*`, and the public `RaftEx` interface.
- Split the catch-all `RaftEx.Lib` into focused domain utilities (`RaftEx.Domain.Uid`, `RaftEx.Domain.Codec`) and infrastructure helpers (`RaftEx.Infra.Filesystem`).
- Separate pure Raft logic in `RaftEx.Server` from its OTP host (`RaftEx.App.ServerRuntime`, formerly `ServerProc`); introduce ports (behaviours) for persistence, network, and timers so the domain depends on abstractions.
- Move persistence (WAL, meta store, segments, snapshots, ETS tables) under `RaftEx.Infra.Persistence.*`; network/distribution under `RaftEx.Infra.Network.*`; supervisors under `RaftEx.Infra.Supervisor.*`; observability under `RaftEx.Infra.Observability.*`.
- Move the OTP application callback out of the domain path (renamed to `RaftEx.OTPApplication`) and keep `RaftEx` as the public facade; update Mix task and tests to new module names.
- Fix naming inconsistencies: `MachineSimple` file typo (`simpile`), `Monitors` -> `Monitor`, and clarify `LeakyIntegrator`/`LruCache` placement.
- Big-bang reorganization: all moves happen in one change; tests are updated to the new structure and are expected to pass only at the end.

## Capabilities

### New Capabilities
(none - pure refactor, no behavior change)

### Modified Capabilities
(none - no spec-level requirement changes)

This change sets `skip_specs: true` in `.openspec.yaml`.

## Impact

- Affected code: every module under `lib/` and all references in `test/`; module paths and names change across the codebase.
- APIs: breaking - public module names (`RaftEx.Types`, `RaftEx.Server`, `RaftEx.Log`, etc.) move to layered namespaces; the `RaftEx` facade function signatures remain stable where feasible but module references change.
- Dependencies: unchanged (`seshat`, `ex_doc`, `excoveralls`, `dialyxir`, `credo`); no new runtime deps.
- Systems: build/test configuration (`mix.exs` `elixirc_paths`, application module reference) updated.