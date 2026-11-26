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

## Segments

Logs are segmented for efficient truncation and compaction. See `LogSegmentWriter`.

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