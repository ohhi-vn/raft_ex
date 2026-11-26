# Observability & Metrics

Metrics are exposed via `RaftEx.Infra.Observability.Metrics` (ETS, always
available) and `RaftEx.Observability` (read API).

```elixir
RaftEx.Observability.metrics()
RaftEx.Observability.metric(:wal_flushes)
RaftEx.Observability.reset()
```

Metrics are updated directly from the calling process (no message round-trip),
so they are safe on hot paths. Counters are atomic under concurrency.

See `docs/metrics.md` for the full catalogue, including replication lag,
backpressure events, and WAL latency.
