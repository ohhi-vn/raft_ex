# Observability & Metrics

Metrics are exposed via `RaftEx.Infra.Observability.Metrics` (ETS, always available)
and `RaftEx.Observability` (read API).

```elixir
RaftEx.Observability.metrics()
RaftEx.Observability.metric(:wal_flushes)
RaftEx.Observability.reset()
```

See `docs/metrics.md` for the full catalogue.