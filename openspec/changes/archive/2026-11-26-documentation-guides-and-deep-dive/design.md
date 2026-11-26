# Design

## Context

ExDoc is configured with `main: "readme"` and only `README.md` + `CHANGELOG.md` as extras. Module docs exist across `lib/`. There is no guides directory. `docs/` holds topical markdown (metrics, persistence invariants, release checklist, wal benchmark) not currently in ExDoc extras. The project uses `mix docs` to generate.

## Goals / Non-Goals

**Goals:**
- Narrative guides discoverable in Hex docs.
- Architecture deep-dive for contributors.
- Cross-links between API docs and guides.
- Doc build is a CI gate (warnings as errors).

**Non-Goals:**
- Translating to other languages.
- Auto-generating guides from specs (manual authoring).
- Hosting a separate docs site.

## Decisions

- **Location**: `guides/` at repo root (ExDoc convention) for narrative guides; keep `docs/` for internal/operational docs. Add both to extras grouped.
- **Structure**: one file per topic; `guides/getting-started.md`, `guides/architecture.md`, etc. Use ExDoc's `groups_for_extras`.
- **Cross-links**: use `{: .tip}` / markdown links to module docs (`RaftEx`, `RaftEx.Domain.Machine`) and to specs directory.
- **CI gate**: add `mix docs --warnings-as-errors` to `quality.ci` alias and release checklist.
- **doctest**: add doctests to key public functions where examples are stable (e.g. `RaftEx.start_in/1`).
- **Versioning**: guides versioned with the library; no separate versioning.

## Risks / Trade-offs

- Guides can drift from code; mitigate by referencing tests and adding doc-build to CI.
- Large doc set is authoring effort; prioritize getting-started + architecture + recovery first.
- ExDoc grouping syntax must match installed ExDoc version.
