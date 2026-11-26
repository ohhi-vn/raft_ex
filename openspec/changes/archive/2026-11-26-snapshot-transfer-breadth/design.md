# Design

## Context

See `proposal.md` for motivation. The relevant code lives in:

- `lib/domain/server/rpc_handler.ex` — follower-side `InstallSnapshot` handling (`handle_install_snapshot_rpc/2`, `install_snapshot/4`, `snapshot_failure/3`).
- `lib/domain/server/server.ex` — leader-side replication (`make_rpc_effect/5` triggers `{:send_snapshot, ...}`) and `handle_leader/2` for `InstallSnapshotResult`.
- `lib/app/server_runtime.ex` — chunked send loop (`handle_effect/5` for `:send_snapshot`, `send_next_snapshot_chunk/4`), tracked in `state.snapshot_sends`.
- `lib/infra/persistence/log_snapshot.ex` — chunk accept/read/recover with CRC32.
- `test/support/distributed_cluster.ex` and `test/integration/distributed_snapshot_transfer_test.exs` — distributed coverage.

The existing spec (`openspec/specs/snapshots`) already defines the externally observable behaviour; this change implements and hardens breadth without changing it.

## Goals / Non-Goals

**Goals:**

- Make all eight distributed scenarios pass reliably.
- Close race windows: term change mid-transfer, superseded transfer, follower disconnect, checksum failure.
- Keep retry implicit via the normal replication pipeline (no new timers).

**Non-Goals:**

- No new public API.
- No changes to the on-disk snapshot format.
- No shared WAL or pipelined AppendEntries (tracked separately).

## Decisions

1. **Follower partial state is ephemeral.** Keep accepting chunks in `state.snapshot_state`/`snapshot_meta` and on disk in `snapshot.dat`. On any failure (term change, corrupt/incomplete, no-in-progress chunk), call `snapshot_failure/3` which deletes `snapshot.dat`, clears both assigns, and replies `{last_index: 0}`. This avoids partial installs and lets the leader retry.

2. **Superseded transfer replaces in flight.** A `:first` chunk arriving while `snapshot_state` is non-nil restarts accept via `LogSnapshot.begin_accept/2` (overwriting the partial file) and updates `snapshot_meta`. No separate "generation" counter is needed because the leader only sends one transfer per follower at a time; a newer snapshot from the same leader simply restarts.

3. **Leader tracks one send state per peer.** `state.snapshot_sends` is a map peer_id → send_state. `handle_effect(:leader, {:send_snapshot, ...})` reuses an existing send_state (resuming) or begins a new read. Disconnect or higher-term reply deletes the entry via `{:clear_snapshot_send, peer_id}`, so the next replication pass re-initiates.

4. **Term change abort is reply-driven plus RPC-driven.** If the follower replies with a higher term, `handle_leader/2` for `InstallSnapshotResult` adopts the term and becomes follower (existing). If a chunk RPC carries a higher term than the follower's current term, `handle_install_snapshot_rpc/2` rejects and clears partial state. The leader stops sending because its `snapshot_sends` entry is cleared on the higher-term reply.

5. **Checksum validation happens at recover time.** `LogSnapshot.complete_accept/2` writes the CRC from the first chunk; `LogSnapshot.recover/1` validates the whole file. A mismatch returns `{:error, :checksum_error}`, routed through `snapshot_failure/3`, so previous state is retained and no acknowledgement is sent.

6. **Replication resumption uses existing pipeline.** After a successful install, the follower sets `next_index` semantics via `Log` (snapshot index + 1) and replies with `last_index`. The leader's `handle_leader/2` updates `match_index`/`next_index` and triggers `:pipeline_rpcs`, which sends `AppendEntries` from snapshot+1. Commit quorum follows automatically from `match_index`.

7. **Distributed tests use `:peer` and are excluded by default.** Mid-transfer fault injection (term change, disconnect) is implemented by driving RPCs directly rather than network partitions where unreliable, with env-gating (`RAFT_EX_FAULTS=1`) for partition scenarios.

## Risks / Trade-offs

- [Race: stale chunk after supersede] → Mitigation: `snapshot_meta` is overwritten on `:first`; middle/last chunks without a matching in-progress state fail with `:no_snapshot_in_progress`.
- [Partial file left on disk after crash] → Mitigation: `snapshot_failure/3` deletes `snapshot.dat`; startup recovery ignores a corrupt/partial file and reports corruption.
- [Retry storms] → Mitigation: retries only happen on the leader's replication tick, not on a timer.
- [Distributed flakiness] → Mitigation: deterministic seeds, bounded retries with sleeps, env-gated fault tests excluded by default.

## Migration Plan

No data migration. Deploy as normal release; existing snapshots remain compatible (format unchanged).

## Open Questions

None.