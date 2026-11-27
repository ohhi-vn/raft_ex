# Quality baseline

This file records the pre-existing static-analysis debt so regressions are
distinguishable from it, as required by tasks 7.5 and 15.5 of
`openspec/changes/raft-core-hardening-and-performance`.

## What passes

```sh
mix compile --warnings-as-errors
mix format --check-formatted
mix test                 # 1262/1262, 27 distributed/chaos tests excluded
mix docs --warnings-as-errors
mix release.check
```

## Dialyzer

`mix dialyzer` emits 150 warnings. None correspond to a failing test, and
the categories are the same ones present before this change (121 warnings):
best-effort side effects whose result is ignored, specs wider than the
success typing, and `no_return` inference on the dense state-map matches
(which transitively marks a handful of private helpers `unused_fun` even
though the test suite exercises them).

| Category | Count |
|----------|------:|
| `unmatched_return` | 73 |
| `contract_supertype` | 23 |
| `no_return` | 13 |
| `call` | 12 |
| `invalid_contract` | 8 |
| `unused_fun` | 7 |
| `pattern_match` / `pattern_match_cov` | 8 |
| `call_without_opaque` | 3 |
| `extra_range` | 2 |
| `guard_fail` | 1 |

## Credo

`mix credo` (default priority gate) exits 10 and `mix credo --strict` exits 10.
Both are **pre-existing**: a clean checkout of `HEAD` also exits 10 (35
refactoring opportunities + 4 design suggestions).

| Run | `HEAD` | After this change |
|-----|-------:|------------------:|
| `mix credo` | exit 10, 35 refactoring + 4 design | exit 10, 100 refactoring + 17 design |
| `mix credo --strict` | exit 10, 35 refactoring + 4 design | exit 10, 100 refactoring + 15 readability + 31 design |
| `[W]` (warning-priority) issues | 0 | 0 |

The added opportunities are concentrated in the new code: `length/1`
comparisons in tests, cyclomatic complexity/nesting in the consensus,
segment-storage and benchmark modules, and explicit `try` style.

Because `mix precommit` runs `credo --strict`, that one step is red on the
baseline and remains red here; every other step of `mix precommit`
(compile with warnings-as-errors, format check, full test suite) passes.

## Policy

- New `credo` **warnings** or new dialyzer categories are regressions.
- The refactoring/design debt should be paid down in a dedicated cleanup
  change, not mixed into functional work.

