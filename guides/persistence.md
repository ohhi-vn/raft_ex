# Persistence & Recovery

## WAL

Each server has its own write-ahead log (WAL) with per-segment files and fsync.
See `RaftEx.Infra.Persistence.LogWal`.

Appends are buffered per server and flushed as one record per batch. A
synchronous append does not return until its entries have been written and the
configured sync method (`:datasync`, `:fsync`, or `:none`) has completed.

Concurrent synchronous appends are group-committed: the first append schedules a
flush, further appends that arrive before it runs join the same batch, and one
write plus one sync covers all of them. Every caller is still acknowledged only
after that shared flush is durable, and receives the index range of its own
entries. If the write or sync fails, every queued caller receives
`{:error, reason}` and the buffered entries are retained for a later retry.

Truncation and conflict replacement are atomic. Prefix truncation (after a
snapshot), suffix truncation, and suffix replacement (follower conflict) write
the retained entries to a temporary file, sync it, rename it over the live
WAL, and sync the containing directory before switching over. A crash leaves
either the complete old file or the complete new file, and a failure returns
`{:error, reason}` without modifying the in-memory log.

WAL files rotate by size. When `wal.dat` reaches `wal_max_size_bytes` (or its
`max_size_bytes` alias), it is renamed to `wal.<seq>.dat` and a fresh
`wal.dat` is started. Configuration accepts both the canonical `wal_*` keys
and the supervisor-style aliases (`:sync_method`, `:compute_checksums`,
`:max_batch_size`, `:max_size_bytes`, `:max_entries`); canonical keys win.
See `RaftEx.Infra.Persistence.LogWal.normalize_config/1`.

Recovery reads every `wal.<seq>.dat` file in numeric order followed by
`wal.dat`, verifies each record's CRC32, rejects implausible lengths without
allocating, decodes with `binary_to_term(..., [:safe])`, and stops at the
first invalid record. Only the last valid prefix is returned.

## Verifying

```sh
mix test test/infra/persistence/log_wal_recovery_test.exs
mix test test/infra/persistence/log_wal_truncate_test.exs
mix test test/infra/persistence/log_wal_replace_test.exs
mix test test/infra/persistence/log_wal_rotation_test.exs
mix test test/infra/persistence/log_wal_config_test.exs
mix test test/infra/persistence/log_wal_error_test.exs
```

## Segments

Logs are segmented for efficient truncation and compaction. See `LogSegmentWriter`.

Segment storage is explicit opt-in (`segment_storage: true`, see
`guides/configuration.md`). When enabled, `Log.seal_oldest/2` writes the
oldest in-memory range to sealed segments and evicts it, so the log keeps a
bounded window while `fold/6`, `sparse_read/2`, and `fetch_term/2` transparently
serve cold entries from disk. Sealing writes first and evicts after, so a crash
between the two leaves recoverable duplicates that deduplicate by index.
Recovery derives each segment's range from its actual content (falling back to a
full parse when the sidecar is missing or stale), drops empty or corrupt files
with a warning, and reports missing or unreadable segments as explicit errors
instead of skipping indexes silently. `delete_up_to/2` removes only segments
entirely at or below the snapshot index; partially covered segments are
retained.

## Snapshots

Snapshots are chunked and transferred between leader and followers. See `LogSnapshot`.

Transfer is resumable: a follower that falls behind receives a snapshot in
first/middle/last chunks, validates the CRC32 checksum on completion, and only
then installs it. A term change, a superseding snapshot, a disconnect, or a
checksum failure discards the partial transfer and retries on the next
replication pass; after installation, AppendEntries resumes from
snapshot index + 1.

## Recovery

On restart, servers recover from WAL + latest snapshot. Invariants are checked via `RaftEx.Domain.Invariant`.