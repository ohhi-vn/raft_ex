# Design

## Context

`Counters` wraps `:seshat` and degrades to no-ops when unavailable. `Metrics` (ETS) already provides counter/gauge/histogram with no external dependency. The codebase has both; `Counters` delegates some operations to `Metrics`. This change consolidates on `Metrics`.

## Goals / Non-Goals

**Goals:**
- Remove `:seshat` dependency entirely.
- Preserve `Counters` public API (overview, counters, fetch, delete, new) as ETS-backed or no-ops.
- Keep metrics always available.

**Non-Goals:**
- Changing metric names or semantics.
- Adding new metrics.

## Decisions

### D1: Counters delegates to Metrics
All `Counters` functions delegate to `Metrics` (ETS). `seshat_available?/0` removed. `init/0` calls `Metrics.init/0`.

### D2: overview/2 returns ETS snapshot
`overview/0` and `overview/1` return `Metrics.all()` (or subset). `counters/2` returns `Metrics.get/1`-style data.

### D3: Per-server counter identity
seshat's per-name counters were keyed by `{group, name}`. ETS uses a flat table; per-server metrics are stored under composite keys `{:counter, {name, server}}` etc. Actually, to keep it simple, `Counters.new/2` and `new/3` become no-ops (registration not needed with ETS auto-create), and `fetch/1`/`delete/1` operate on Metrics keys.

## Risks / Trade-offs

- [Callers relying on seshat-specific return shapes] → `overview/0` previously returned seshat's map; now returns `Metrics.all()` map. Tests updated.
- [Performance] → ETS is fast; no regression expected.