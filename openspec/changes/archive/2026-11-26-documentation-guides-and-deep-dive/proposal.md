# Proposal

## Why

Generated API docs and behaviour docs exist (via ExDoc), but there are no guides or an architecture deep-dive. New users must read source to understand cluster lifecycle, persistence, recovery, membership changes, and consistent queries. The README points to `openspec/specs/` for behaviour but not to narrative documentation. This is the last "In Progress" documentation gap.

## What Changes

- Add a `guides/` (or `docs/guides/`) set of narrative guides: Quick Start, Cluster Lifecycle, State Machines, Persistence & Recovery, Membership Changes, Snapshots, Consistent Queries, Leadership Transfer, Observability/Metrics, Configuration, Troubleshooting.
- Add an architecture deep-dive document covering the gen_statem lifecycle, domain/infra layering, effects system, RPC routing, and data flow (command → log → state machine → reply).
- Wire guides into ExDoc (`extras` + `groups_for_extras`) so they appear in Hex docs; add a `docs` task check to precommit/CI.
- Add behaviour docs cross-links from `@moduledoc`/`@doc` to the relevant guides.
- Add a documentation coverage/quality check (ex_doc warnings as errors) in CI.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

(none)

No spec-level behaviour changes; documentation only. `skip_specs: true` is set in `.openspec.yaml`.

## Impact

- Affected code: `docs/`, `guides/`, `mix.exs` (docs config), module docs in `lib/`.
- APIs: none.
- Tests: doc tests (`doctest`) where feasible; ExDoc build in CI.
- Dependencies: none new.
