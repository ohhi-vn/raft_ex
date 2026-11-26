# Configuration

RaftEx is configured through a system config map and per-server options.

## System config

Common keys (see `RaftEx.App.System.default_config/0`):

- `:data_dir` — base directory for WAL, snapshots, and meta files.
- `:wal_sync_method` — `:datasync`, `:fsync`, or `:none`.
- `:snapshot_chunk_size` — bytes per snapshot chunk (default 65536).
- `:broadcast_time` — heartbeat interval in ms (default 100).
- `:election_timeout_min` / `:election_timeout_max` — election timeout range in ms.
- `:tick_timeout` — server tick interval in ms (default 1000).
- `:max_pipeline_count` — in-flight AppendEntries limit per follower (default 4096).
- `:max_append_entries_rpc_batch_size` — entries per AppendEntries batch (default 128).
- `:command_plugs` / `:rpc_plugs` — plug pipelines (see `guides/` plug docs).

## Per-server config

Passed to `RaftEx.start_cluster/4` or `RaftEx.start/1`:

- `:machine` — `{module, initial_state}` or `{:module, mod, args}`.
- `:initial_members` — list of server ids in the initial cluster.
- `:uid` — unique id for this server (used for per-server WAL/meta).

## Example

```elixir
system = RaftEx.App.System.default_config()
  |> Map.put(:data_dir, "/var/lib/raft_ex")
  |> Map.put(:wal_sync_method, :datasync)

RaftEx.start(system: system)
```
