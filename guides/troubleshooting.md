# Troubleshooting / FAQ

- **Cluster not forming**: ensure all server ids use the same node or
  distributed Erlang is started (`elixir --sname ...`).
- **No leader**: check election timeouts and connectivity; inspect logs
  for `pre_vote`/`candidate` transitions.
- **Stale reads**: use `consistent_query/3` instead of `local_query/3`.
- **WAL recovery**: delete the data dir to start fresh (dev only).
- **Slow replication**: check `replication_backpressure` metric and
  `max_pipeline_count` setting.
