# Design

## Context

See proposal.md - Why. Current code lives flat under `RaftEx.*` with no enforced dependency direction. The two largest modules (`RaftEx.Server` ~1.2k lines of pure logic, `RaftEx.ServerProc` ~0.9k lines of gen_statem glue) are the main coupling hotspot: domain decisions and OTP effects are interleaved through shared state maps. Infrastructure (ETS/DETS, `:prim_file`, `:rpc`, `Node`, supervisors) is called directly from domain-adjacent code.

Constraints:
- Elixir/OTP: module names must be valid atoms; `mix.exs` references `RaftEx.Application` as the OTP app callback. Renaming it requires updating `application/0`.
- Tests reference many internal modules directly (`RaftEx.Types`, `RaftEx.Server`, `RaftEx.Server.Cluster`, `RaftEx.Log`, etc.); a big-bang rename means tests move with the code.
- No new runtime dependencies; keep `seshat`, `ex_doc`, `excoveralls`, `dialyxir`, `credo`.
- `elixirc_paths` includes `test/fixtures` in test env; preserve.

## Goals / Non-Goals

**Goals:**
- Enforce a four-layer dependency rule: Domain <- Application <- Infrastructure <- Interface (outer depends on inner; inner never imports outer).
- Give each module a single, layer-appropriate home with a consistent naming scheme.
- Make the Raft core (`Domain.Server` and collaborators) pure and unit-testable without starting OTP.
- Introduce ports (behaviours) for persistence, network transport, timers, and process spawning so domain/application depend on abstractions.
- Keep the `RaftEx` facade as the stable-ish entry point for callers, while permitting breaking module renames.

**Non-Goals:**
- Rewriting the Raft algorithm itself or changing observable behavior.
- Changing on-disk formats, wire protocol, or DETS/ETS table layouts.
- Performance work (gen_batch_server, pipelining) - out of scope; preserve current characteristics.
- Splitting into separate Hex packages.

## Decisions

### D1 - Layered namespace

Adopt four namespaces:
- `RaftEx.Domain.*` - pure logic, no IO, no OTP, no process calls. May depend only on stdlib/Elixir.
- `RaftEx.App.*` - use cases and OTP process hosts (gen_statem/gen_server). Depends on Domain + ports.
- `RaftEx.Infra.*` - adapters implementing ports (persistence, network, supervisors, observability, registry, config).
- `RaftEx` (top-level) + `RaftEx.Interface.*` - public facade and Mix tasks.

Rationale: matches Clean Architecture terminology while staying within one app; avoids an extra package. Alternative (separate apps per layer) rejected: too much ceremony for a single library and breaks the existing single-package distribution model.

### D2 - Module placement (representative mapping)

| Current | New | Layer |
|---|---|---|
| `RaftEx.Types` | `RaftEx.Domain.Types` | Domain |
| `RaftEx.Server` | `RaftEx.Domain.Server` | Domain |
| `RaftEx.Server.Config` | `RaftEx.Domain.Server.Config` | Domain |
| `RaftEx.Server.Cluster` | `RaftEx.Domain.Server.Cluster` | Domain |
| `RaftEx.Server.Effects` | `RaftEx.Domain.Server.Effects` | Domain |
| `RaftEx.Server.Election` | `RaftEx.Domain.Server.Election` | Domain |
| `RaftEx.Server.Rpc.RpcHandler` | `RaftEx.Domain.Server.RpcHandler` | Domain |
| `RaftEx.Machine` | `RaftEx.Domain.Machine` | Domain |
| `RaftEx.MachineSimple` | `RaftEx.Domain.Machine.Simple` | Domain |
| `RaftEx.Range`, `Seq`, `LeakyIntegrator`, `LruCache` | `RaftEx.Domain.*` | Domain |
| `RaftEx.Lib` (pure parts) | `RaftEx.Domain.Uid`, `RaftEx.Domain.Codec` | Domain |
| `RaftEx.ServerProc` | `RaftEx.App.ServerRuntime` | Application |
| `RaftEx.System` | `RaftEx.App.System` | Application |
| `RaftEx.Cluster` | `RaftEx.App.Cluster` | Application |
| `RaftEx.SystemRecover` | `RaftEx.App.Recovery` | Application |
| `RaftEx.Log` (orchestration) | `RaftEx.Infra.Persistence.Log` | Infrastructure |
| `RaftEx.LogWal`, `LogMeta`, `LogEts` | `RaftEx.Infra.Persistence.*` | Infrastructure |
| `RaftEx.LogSegmentWriter`, `LogSnapshot*`, `LogPreInit` | `RaftEx.Infra.Persistence.*` | Infrastructure |
| `RaftEx.Snapshot` | `RaftEx.Infra.Persistence.Snapshot` | Infrastructure |
| `RaftEx.File` | `RaftEx.Infra.Filesystem` | Infrastructure |
| `RaftEx.Directory` | `RaftEx.Infra.Registry.Directory` | Infrastructure |
| `RaftEx.Network` | `RaftEx.Infra.Network.Transport` | Infrastructure |
| `RaftEx.NodeMonitor` | `RaftEx.Infra.Network.NodeMonitor` | Infrastructure |
| `RaftEx.Distribution` | `RaftEx.Infra.Network.Distribution` | Infrastructure |
| `RaftEx.Counters`, `MetricsEts`, `Monitors` | `RaftEx.Infra.Observability.*` | Infrastructure |
| `RaftEx.Supervisor`, `*Supervisor` | `RaftEx.Infra.Supervisor.*` | Infrastructure |
| `RaftEx.Env` | `RaftEx.Infra.Config.Env` | Infrastructure |
| `RaftEx.Application` (OTP callback) | `RaftEx.OTPApplication` | Infrastructure/Boot |
| `RaftEx` (facade) | `RaftEx` (unchanged top-level) | Interface |
| `Mix.Tasks.RaftEx.Cluster` | `Mix.Tasks.RaftEx.Cluster` (unchanged) | Interface |

`LeakyIntegrator` and `LruCache` are pure data structures -> Domain. `Worker` (queue) reviewed during move; keep in Domain if pure else Infra.

### D3 - Ports (behaviours) for inverted dependencies

Define in `RaftEx.App.Ports` (or `RaftEx.Domain.Ports` for domain-level ones):
- `Persistence` - append/read/truncate/snapshot of log entries and metadata.
- `Transport` - send RPC, monitor nodes.
- `Timer` / `Clock` - scheduling heartbeats/elections and reading time (domain currently uses `System.system_time` indirectly; wrap).
- `ProcessSpawner` - start/stop supervised children.

Domain code depends only on port behaviours and injected adapters; no direct ETS/file/RPC calls. Application layer wires concrete adapters from `RaftEx.Infra.*`.

Rationale: enables unit testing the Raft core with fakes. Alternative (keep direct calls but move modules) rejected: does not achieve testability goal and leaves hidden coupling.

### D4 - Keep domain pure

`RaftEx.Domain.Server.init/1` and state transitions remain pure functions returning `{state, effects}`. Effects are data; execution stays in `RaftEx.App.ServerRuntime`. No `Logger`, `:ets`, `File`, or `Node` calls inside `Domain.*`. Logging moves to application/infra adapters or is emitted as effects.

### D5 - Facade stability

`RaftEx` keeps its public function set and delegates to new modules. Internal module paths break (acceptable). Tests updated to new paths. `RaftEx.Machine` behaviour moves to `RaftEx.Domain.Machine`; a deprecated alias `RaftEx.Machine` may be retained temporarily to ease external callers, marked `@deprecated`.

### D6 - Naming fixes

- File `lib/machine/machine_simpile.ex` -> `lib/domain/machine/simple.ex`; module `RaftEx.Domain.Machine.Simple`.
- `RaftEx.Monitors` -> `RaftEx.Infra.Observability.Monitor` (singular).
- `RaftEx.Lib` removed; its functions redistributed.

### D7 - No spec changes

Pure refactor: `skip_specs: true`. Behavior, on-disk format, and wire protocol unchanged.

## Risks / Trade-offs

- [Big-bang rename breaks all tests until the end] -> Mitigation: do moves in dependency order (Domain first, then App, then Infra, then Interface), run `mix compile` after each layer; keep a checklist in tasks.md.
- [Missed references cause compile failures] -> Mitigation: use `grep`/`rg` for old module names before and after; add a CI step `grep -R "RaftEx\.\(Types\|Server\|Log\|...\)" lib test` gate.
- [Port abstraction leaks OTP concepts] -> Mitigation: define port callbacks with plain data terms; adapters translate to OTP.
- [Loss of institutional knowledge in module names] -> Mitigation: keep `@moduledoc` migration notes and a mapping table in design.md (this doc).
- [Dialyzer/credo config paths] -> Mitigation: update `.dialyzer_ignore.exs` and credo config if any.

## Migration Plan

1. Create new directory skeleton (`lib/domain`, `lib/app`, `lib/infra`, `lib/interface`) - empty.
2. Move Domain modules first; update internal aliases; compile.
3. Define ports in `RaftEx.App.Ports`; refactor domain to depend on ports (inject adapters).
4. Move Application modules; wire ports to infra adapters.
5. Move Infrastructure modules; update supervisors and `mix.exs` application callback.
6. Update `RaftEx` facade and Mix task.
7. Update tests and fixtures; run full suite.
8. Run `mix format`, `credo --strict`, `dialyzer`; fix.
9. Update README module references and CHANGELOG.

Rollback: single git commit per layer allows reverting; since big-bang, final rollback is reverting the whole change branch.

## Open Questions

(none)