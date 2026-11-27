# Troubleshooting / FAQ

- **Cluster not forming**: ensure all server ids use the same node or
  distributed Erlang is started (`elixir --sname ...`).
- **No leader**: check election timeouts and connectivity; inspect logs
  for `pre_vote`/`candidate` transitions.
- **Stale reads**: use `consistent_query/3` instead of `local_query/3`.
- **WAL recovery**: delete the data dir to start fresh (dev only).
- **Slow replication**: check `replication_backpressure` metric and
  `max_pipeline_count` setting.
- **Commands rejected with `:backpressure`**: the leader's pending-command
  queue (`max_pending_commands`, default 1024) is full. Add capacity, shed
  load, or raise the limit; the queue drains in batches.
- **Peer stuck suspect after a partition**: the leader marks peers on a
  down node `{:suspect, _}` and retries them after
  `node_suspect_timeout_ms` (default 30 s) or immediately on node-up.
  Check `NodeMonitor` and network connectivity.
- **No automatic snapshots**: automatic compaction needs one threshold to
  fire (`compaction_entry_threshold`, `compaction_wal_bytes`,
  `compaction_interval_ms`); transfers in flight defer it. Use
  `RaftEx.trigger_compaction/1` to force one. With segment storage on,
  compactions also seal and evict; check the server log for
  `checkpoint failed` errors.
- **Segment read errors**: `{:segment_missing, ...}` means a segment file
  was deleted out of band — restore it or snapshot; `{:segment_corrupt,
  ...}` means checksum/parse failure — the previous snapshot and WAL are
  still authoritative, restart to recover them.
