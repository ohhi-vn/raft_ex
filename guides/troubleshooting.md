# Troubleshooting / FAQ

- **Cluster not forming**: ensure all server ids use the same node or distributed Erlang is started.
- **No leader**: check election timeouts and connectivity.
- **Stale reads**: use `consistent_query/3` instead of `local_query/3`.
- **WAL recovery**: delete data dir to start fresh (dev only).