# Proposal

## Why

RaftEx declares `:seshat` as a runtime dependency in `mix.exs`, but the library
only uses it as an optional counter backend behind `RaftEx.Infra.Observability.Counters`.
The `Counters` module already degrades gracefully when seshat is unavailable, and a
pure-Elixir ETS-backed `RaftEx.Infra.Observability.Metrics` module now exists with
counter/gauge/histogram support. Keeping `:seshat` forces an external dependency on
all users and adds a native-ish dependency for functionality that Elixir's ETS can
provide directly. This change removes the seshat dependency and migrates all counter
usage to the ETS-backed Metrics module.

## What Changes

- Remove `{:seshat, "~> 0.6"}` from `mix.exs` deps.
- Migrate `RaftEx.Infra.Observability.Counters` to delegate to `Metrics` (ETS) instead of seshat; drop seshat availability checks.
- Remove seshat-specific init (`:seshat.new_group`, persistent_term fields spec) and rely on `Metrics.init/0`.
- Update `MetricsEts` to no longer call `Counters.init/0` seshat path.
- Update tests that reference seshat.
- Update README/docs to reflect the removed dependency.

## Capabilities

No spec-level behavior changes; metrics are an implementation/observability detail.
`skip_specs: true` is set in `.openspec.yaml`.

## Impact

- Affected code: `mix.exs`, `lib/infra/observability/counters.ex`, `lib/infra/observability/metrics_ets.ex`, tests.
- Dependencies: removes `:seshat`.
- APIs: `Counters` public API preserved (overview/counters/fetch/delete become ETS-backed or no-ops); `Metrics` becomes the primary store.
- Tests: existing observability tests must pass without seshat.