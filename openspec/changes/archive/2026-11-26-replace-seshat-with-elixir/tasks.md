# Tasks

## 1. Remove seshat dependency

- [x] 1.1 Remove `{:seshat, "~> 0.6"}` from `mix.exs` deps; verify `mix deps.get` succeeds
- [x] 1.2 Update `lib/infra/observability/metrics_ets.ex` to not call seshat init; verify compile

## 2. Migrate Counters to ETS

- [x] 2.1 Rewrite `Counters` to delegate to `Metrics` (ETS); remove `seshat_available?/0`; verify compile
- [x] 2.2 Make `new/2`, `new/3`, `fetch/1`, `delete/1` ETS-backed; verify unit tests
- [x] 2.3 Make `overview/0` and `overview/1` return `Metrics.all()` (or subset); verify tests

## 3. Tests and docs

- [x] 3.1 Update `test/infra/observability_test.exs` for ETS-backed counters; verify pass
- [x] 3.2 Update README/docs to remove seshat references; verify `mix docs` clean

## 4. Verification

- [x] 4.1 Run `mix precommit`; verify 0 failures
- [x] 4.2 Run `mix deps.get` and confirm no seshat in lockfile