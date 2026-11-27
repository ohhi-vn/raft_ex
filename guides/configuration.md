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
- `:max_pending_commands` — bounded pending-command queue depth (default 1024); overflow returns `:backpressure`.
- `:command_batch_linger_ms` — batching window for grouping concurrent commands (default 0).
- `:command_plugs` / `:rpc_plugs` — plug pipelines (see `guides/` plug docs).

## Compaction

Automatic compaction snapshots and truncates the log once any threshold is
reached (checked on every apply batch and on every leader tick):

- `:compaction_entry_threshold` — applied entries since the last snapshot (default 8192).
- `:compaction_wal_bytes` — WAL bytes under the server directory (default 134217728).
- `:compaction_interval_ms` — milliseconds since the last snapshot (default 120000).
- `:node_suspect_timeout_ms` — how long a partitioned peer stays suspect before a retry probe (default 30000).

Set any threshold to `nil` to disable that trigger. Compaction defers while
a snapshot transfer is in progress and retries on the next check.
`RaftEx.trigger_compaction/1` forces a snapshot immediately regardless of
thresholds.

## Segment-backed storage

Segment storage keeps only a bounded in-memory window and serves older
entries from sealed segments:

- `:segment_storage` — `true` to enable, `false` (default) to keep the full log in memory.
- `:log_window_size` — in-memory entry budget used with segment queries (default 8192).
- `:segment_max_entries` / `:segment_max_size_bytes` — active segment rotation limits.
- `:segment_compute_checksums` — CRC sidecars for sealed segments.

Segments are an explicit opt-in until the read/recovery/compaction suites
pass with them enabled; the default keeps the battle-tested in-memory log.

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
