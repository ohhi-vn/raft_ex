# Persistence invariants

RaftEx maintains five indices that must stay consistent across normal
operation and crash/recovery:

| Index | Meaning |
|-------|---------|
| `snapshot_index` | Highest index captured in the latest snapshot |
| `wal_persisted_index` | Highest index durably written to the WAL |
| `log_last_index` | Highest index present in the in-memory log |
| `commit_index` | Highest index known to be committed |
| `last_applied` | Highest index applied to the state machine |

## Invariants

1. `snapshot_index <= wal_persisted_index` — the WAL contains at least the snapshot.
2. `wal_persisted_index <= log_last_index` (steady state) — the in-memory log is at least as far as the WAL. May be temporarily violated after truncating a conflicting suffix before the WAL is truncated.
3. `commit_index <= log_last_index` — cannot commit beyond what is logged.
4. `last_applied <= commit_index` — cannot apply beyond what is committed.
5. `snapshot_index <= last_applied` (when a snapshot exists) — the state machine reflects at least the snapshot.

## Recovery ordering

1. Load the latest snapshot (sets `snapshot_index`, `last_applied = snapshot_index`).
2. Replay the WAL from `snapshot_index + 1` (rebuilds in-memory log; `log_last_index = wal_persisted_index`).
3. Restore metadata (`current_term`, `voted_for`, `last_applied`, `commit_index`).
4. Recompute `commit_index = max(last_applied, snapshot_index)`.
5. Apply committed entries up to `commit_index`.
6. Assert all invariants via `RaftEx.Domain.Invariant.check_recovery!/1`.

Re-running recovery on the same data directory yields identical state (idempotent).

## Atomic truncation and replacement

Prefix truncation (snapshot compaction), suffix truncation, and suffix
replacement (follower conflict) all rewrite through a temporary file:
write retained entries to `<wal>.tmp`, sync the file, atomically rename it
over the live WAL, sync the containing directory (best-effort on platforms
where the file driver refuses directories), then reopen the WAL. The
original file descriptor is kept open until the rename succeeds, so a crash
leaves either the complete old file or the complete new file — never a
torn WAL. A failed rewrite reports `{:error, reason}` and leaves the
original file, the in-memory log, and all pending acknowledgements
untouched.

## Rotation

When the active WAL reaches `wal_max_size_bytes` (or its
`max_size_bytes` alias, default 256 MB), it is renamed to `wal.<seq>.dat`
and a fresh `wal.dat` is started. Recovery reads every `wal.<seq>.dat`
file in numeric order followed by `wal.dat`, so entries are returned once
in ascending index order across rotations. Truncation and replacement
consolidate all retained entries into `wal.dat` and delete superseded
rotated files.

## Recovery validation

Recovery reads each record's header, rejects implausible blob lengths
without allocating, verifies the CRC32 over header and payload when
checksums are enabled, decodes with `binary_to_term(..., [:safe])`, and
stops at the first invalid record — a torn tail, an empty payload, a
checksum mismatch, or an undecodable blob. Only the last valid prefix is
returned, and recovery never raises on corrupt input.

## Segments and compaction

- Sealed segments are immutable; the active segment is the only writer.
  Sealing writes the new header, syncs, then writes the sidecar index, so
  a crash lands on either the complete old segment or a recoverable new
  one (the sidecar checksum decides; a mismatch falls back to a full
  parse, and the range is derived from actual content).
- `Log.seal_oldest/2` writes entries to segments and syncs *before*
  evicting them from memory. A crash between the two leaves duplicates
  that deduplicate by index — never a gap.
- `LogSegmentWriter.read/3` returns `{:error, {:segment_missing, ...}}`
  for deleted files and `{:error, {:segment_corrupt, ...}}` for
  undecodable entries; the active (unsealed) segment never reports
  corruption for its not-yet-written tail.
- Automatic compaction fires on entry count, WAL bytes, or elapsed time
  (see `guides/configuration.md`), defers while a snapshot transfer is
  in flight, and produces exactly one snapshot per trigger; the operator
  override `RaftEx.trigger_compaction/1` always works.