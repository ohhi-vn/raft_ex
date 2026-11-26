# Persistence & Recovery

## WAL

Each server has its own write-ahead log (WAL) with per-segment files and fsync.
See `RaftEx.Infra.Persistence.LogWal`.

## Segments

Logs are segmented for efficient truncation and compaction. See `LogSegmentWriter`.

## Snapshots

Snapshots are chunked and transferred between leader and followers. See `LogSnapshot`.

## Recovery

On restart, servers recover from WAL + latest snapshot. Invariants are checked via `RaftEx.Domain.Invariant`.