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