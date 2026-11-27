# Metrics catalogue

RaftEx exposes metrics through two mechanisms:

1. `RaftEx.Infra.Observability.Counters` — compatibility layer over the ETS-backed `Metrics` store.
2. `RaftEx.Infra.Observability.Metrics` — always-available ETS-backed counters/gauges/histograms.

The canonical list of metric names and kinds lives in
`RaftEx.Infra.Observability.MetricNames`.

## ETS metrics (`RaftEx.Infra.Observability.Metrics`)

| Name | Kind | Unit | Description |
|------|------|------|-------------|
| `:wal_flushes` | counter | count | Number of WAL flush operations performed |
| `:wal_flush_latency_ms` | histogram | ms | Latency of a WAL flush (ms), including fsync |
| `:wal_bytes_written` | counter | bytes | Bytes written to the WAL |
| `:wal_segment_writes` | counter | count | Number of WAL segment writes |
| `:wal_fsyncs` | counter | count | Number of WAL fsync/datasync calls |
| `:elections_started` | counter | count | Number of elections started |
| `:elections_won` | counter | count | Number of elections won |
| `:elections_lost` | counter | count | Number of elections lost |
| `:term_changed` | counter | count | Number of term increments |
| `:votes_granted` | counter | count | Number of votes granted |
| `:quorum_reached` | counter | count | Number of times quorum was reached |
| `:commit_advanced` | counter | count | Number of commit-index advancements |
| `:commit_index` | gauge | index | Current commit index |
| `:replication_backpressure` | counter | count | Number of replication backpressure events |
| `:append_entries_sent` | counter | count | Number of AppendEntries RPCs sent |
| `:append_entries_acked` | counter | count | Number of AppendEntries RPCs acknowledged |
| `:replication_lag` | gauge | entries | Per-follower replication lag as an entry-index gap (keyed by follower id) |
| `:snapshot_chunks_sent` | counter | count | Snapshot chunks sent |
| `:snapshot_chunks_received` | counter | count | Snapshot chunks received |
| `:snapshot_chunks_installed` | counter | count | Snapshots installed |
| `:snapshot_chunks_failed` | counter | count | Snapshot chunk failures |
| `:snapshot_transfer_duration_ms` | histogram | ms | Snapshot transfer duration |
| `:leadership_transfers_initiated` | counter | count | Leadership transfers initiated |
| `:leadership_transfers_completed` | counter | count | Leadership transfers completed |
| `:leadership_transfers_failed` | counter | count | Leadership transfers failed |

## Reading metrics

```elixir
RaftEx.Infra.Observability.Metrics.all()
RaftEx.Infra.Observability.Metrics.get(:wal_flushes)
RaftEx.Observability.metrics()
RaftEx.Observability.metric(:commit_index)
```

## Stores

- The ETS store is flat (metric name only) and always available.
- `Counters` is a thin compatibility layer over the ETS store; there is no
  external dependency. `counter_fields/0` lists the per-server field spec.

## Atomicity

- Counters use `:ets.update_counter/4` with a default tuple, so concurrent
  increments are atomic and never lose an update.
- Histogram count and sum are updated together through a single
  `:ets.update_counter/4` call for integer observations; min and max are
  best-effort and may reflect a stale extreme under concurrency.
- `Metrics.init/0` is idempotent and retries table creation on a race.