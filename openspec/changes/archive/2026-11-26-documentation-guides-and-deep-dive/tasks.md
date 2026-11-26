# Tasks

## 1. Guide scaffolding

- [x] 1.1 Create `guides/` directory with index/overview page.
- [x] 1.2 Add getting-started guide (install, start system, run cluster, send command).
- [x] 1.3 Add state-machine guide (behaviour, apply, tick, version, aux).
- [x] 1.4 Add architecture deep-dive (gen_statem, layers, effects, RPC routing, data flow).

## 2. Topical guides

- [x] 2.1 Persistence & recovery (WAL, segments, snapshots, invariants).
- [x] 2.2 Membership changes (voter/promotable/non-voter, join/leave).
- [x] 2.3 Consistent queries.
- [x] 2.4 Leadership transfer.
- [x] 2.5 Observability & metrics.
- [x] 2.6 Configuration reference.
- [x] 2.7 Troubleshooting / FAQ.

## 3. ExDoc integration

- [x] 3.1 Update `mix.exs` docs config: extras + `groups_for_extras`.
- [x] 3.2 Include `docs/*.md` operational docs where appropriate.
- [x] 3.3 Add cross-links in module `@moduledoc`s.

## 4. Quality gates

- [x] 4.1 Add `mix docs --warnings-as-errors` to `quality.ci` and release checklist.
- [x] 4.2 Add doctests to stable public APIs.
- [x] 4.3 Verify `mix docs` builds cleanly.
- [x] 4.4 Update README docs section and feature matrix.
