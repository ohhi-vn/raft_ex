# Design

## Context

See `proposal.md` for motivation and `specs/` for required behavior. The current implementation is a functional core (`RaftEx.Domain.Server`) driven by a `gen_statem` runtime (`RaftEx.App.ServerRuntime`) over a per-system WAL process (`RaftEx.Infra.Persistence.LogWal`), a DETS metadata store (`LogMeta`), a file snapshot store (`LogSnapshot`), and a transport module that sends directly from the server process. The review found the defects listed below; each decision here is shaped by one of them.

Constraints that shape the approach:

- The WAL and snapshot on-disk formats are already in use and must remain readable.
- `RaftEx.Domain.Server` is a pure functional core; side effects are returned as effect tuples and interpreted by `ServerRuntime`. New behavior should follow that split.
- One WAL process serves every uid in a system; one `gen_statem` per server owns all consensus work.
- Public APIs in `RaftEx` should keep their shapes; only failure-path results and the legacy `:ra` fallback change.
- The change is large, so it must land in independently verifiable phases with the existing suite green after each.

## Goals / Non-Goals

**Goals:**

- Make every durability transition (term/vote, WAL append, WAL truncate, snapshot install, compaction) atomic or fail-closed.
- Remove dead-end runtime states and dropped effects so the server always makes progress or fails explicitly.
- Make command throughput proportional to batches rather than to individual fsyncs, and make follower catch-up independent of client traffic.
- Keep the log's memory and disk footprint bounded through segments and automatic compaction.
- Produce benchmarks that measure the real end-to-end path and fail on misconfigured scenarios.

**Non-Goals:**

- Changing the Raft protocol or public API shapes.
- Introducing a new consensus algorithm, joint-consensus membership, or multi-raft sharding.
- Replacing DETS with a new database; the design narrows DETS to term/vote and makes apply progress asynchronous.
- Optimizing the Erlang distribution layer itself (compression, TLS, custom protocols).
- Tuning OS or filesystem parameters.

## Decisions

### 1. Separate "adopt a higher term" from "observe the current term"

`update_term/2` currently runs on `leader_term >= current_term` and unconditionally clears `voted_for`. Split it into:

- `adopt_term(state, new_term)` when `new_term > current_term`: persist the term, clear the vote, clear leader/transfer state.
- `observe_term(state, term)` when `term == current_term`: update `leader_id` and reset the election timeout only; never touch `voted_for`.

Persistence returns `:ok | {:error, reason}`. Callers that must reply (vote grant, heartbeat, snapshot) refuse to reply on error and log a safety event. `LogMeta.store_sync/4` must stop swallowing `:dets` results and must reply to waiters with the real outcome.

Alternative considered: keep one function with a guard. Rejected because every call site would need to remember the guard; the two-name split makes the safety rule impossible to miss.

### 2. Make WAL truncation a single atomic rewrite operation

Add `LogWal.replace_from(wal, uid, from_index, entries)` and `LogWal.truncate_prefix(wal, uid, through_index)`:

1. Write retained records to `<wal>.tmp`.
2. `:file.sync` the temp file.
3. `:file.rename` over the active file.
4. `:file.sync` the containing directory.
5. Only then update the in-memory index and reply.

Suffix replacement and prefix truncation both use this path. Offset-based in-place truncation is a possible optimization but not the correctness mechanism. The existing append-mode `:file.truncate` call is removed entirely.

Alternative considered: `:file.truncate` on a second, non-append fd at the correct offset. Faster, but a crash between truncate and index update can leave the file and the recovered range inconsistent, and it does not handle conflict replacement. The rewrite is O(retained bytes) and conflicts are rare; correctness wins.

### 3. CRC-verified, bounded WAL recovery

Recovery reads the header, validates a maximum record length, reads exactly that many bytes (handling `:eof` and short reads), verifies the stored CRC over the payload, and decodes with `binary_to_term(..., [:safe])`. The first invalid record stops recovery; everything before it is kept. The same rules apply to every rotated file, read in order.

Alternative considered: keep ignoring CRCs and only handle `:eof`. Rejected: silent bit flips become state-machine divergence, and the spec now requires checksum verification.

### 4. Snapshot transfer as a framed, acknowledged stream

Use one chunk frame for all chunk types: `<<crc::32, payload::binary>>`, where the CRC covers the payload only. The receiver strips the frame once in a shared `accept_chunk/2` used by first/middle/last, so the CRC can never leak into the snapshot body.

Replace the `:receive_snapshot` catch-all with real handlers that:

- keep the open temp file and expected index/term in state,
- accept `:middle` and `:last` chunks,
- abort on a term change, checksum failure, timeout, or superseding snapshot,
- reply only after `complete_accept` has synced and renamed the temp file.

A per-transfer id (snapshot index + term + leader id) lets the follower reject stale chunks. The leader keeps one chunk in flight per peer and advances only on acknowledgement, with a retry timer. `snapshot_sends` is cleared on completion, abort, and timeout.

Alternative considered: keep sending all chunks in one `:next_event` loop. Rejected: it has no flow control, no acknowledgement, and no way to abort promptly; the current loop also never resets peer state.

### 5. Atomic snapshot installation that preserves the previous snapshot

`LogSnapshot` writes `snapshot.dat.tmp`, syncs it, renames it to `snapshot.dat`, and syncs the directory. The previous `snapshot.dat` is only replaced by the rename. A failed or stale transfer deletes only the temp file. `Log.compact_after_snapshot` runs only after the rename and directory sync succeed.

Snapshot metadata gains a versioned `cluster` field. `Server.init_from_snapshot` restores it when present and falls back to `initial_nodes` for legacy snapshots.

### 6. Fail-closed follower append

`RpcHandler.append_entries/3` must not build a success reply after a failed write. The order becomes:

1. Validate the consistency prefix.
2. Call the atomic `replace_from` for the replacement suffix.
3. On success, build the success reply; on failure, keep the old log and build a failure reply with the current next-index hint.

This also removes the current "truncate in memory, then write, then ignore the error" sequence.

### 7. Real runtime states instead of catch-alls

- `await_condition`: store the predicate and a retry interval on entry; on each retry evaluate the predicate, retry the blocked operation, and step down or reply with an error when the timeout expires. No state may ignore all events.
- `receive_snapshot`: handled per decision 4.
- `terminating_*`: either implement real cleanup or return `{:stop, :normal, state}`.
- Unknown events: log with state and event type, and reply to calls with `{:error, :unsupported}` instead of leaving callers to time out.

### 8. Apply election effects in the runtime

Add explicit `handle_effect` clauses for `{:update_term, term}` and `{:become, role}`. Both persist the term before changing state. `{:become, :leader}` already exists; the follower and candidate paths use the same helper. `TimeoutNow` is validated against the recognized leader and persisted before the election starts.

### 9. Route replies and notifications by effect shape

`Effects.add_reply/5` already distinguishes `:leader`, `:local`, `{:member, id}`, and `{:notify, corr, pid}`. The runtime must handle:

- `{:reply, from, reply}` — existing,
- `{:reply, from, reply, replier}` — send to the originating caller with the replier identity attached,
- `{:notify, notifys}` — send one `{:pipeline_notify, corr, reply}` per entry after the batch is durable.

`pending_notifys` becomes a real queue flushed with the batch rather than dead state.

### 10. Terminate `delete_cluster` explicitly

Replace the `{:delete_and_terminate, ...}` pseudo-state with a runtime `{:stop, :normal, state}` after data cleanup, and remove the child from the supervisor so it is not restarted. The delete log entry remains replicated so other members remove the server; the local process exits once its data is gone.

### 11. Persist server configuration atomically

`start_server/start_cluster` writes `<data_dir>/<uid>/config` via temp + sync + rename. The file records id, uid, cluster, machine module/version, and initial members. `ServerSupSupervisor.recover_config/2` reads it; `Recovery` reports a missing config instead of warning and skipping.

### 12. Batch commands in the runtime, not in the WAL

Add a bounded pending-command queue to `ServerRuntime`. Each `{:call, from}` command is appended to the queue; a zero-delay `:next_event` drains the queue into one `Server.append_commands/2` call, which builds all entries, performs one `Log.write/2`, and returns one effect set. Replies are correlated to the original `from` by index.

The WAL keeps its group commit as a second line of defense for concurrent appends from different servers. The queue bound is configurable; overflow replies `{:error, :busy}`.

Alternative considered: a dedicated batcher process. Rejected: it adds a hop and a second ordering authority; the `gen_statem` already serializes commands and can batch with a `:next_event`.

### 13. Use the configured replication batch size and pipeline on acknowledgement

`Server.make_rpcs/1` passes `max_append_entries_rpc_batch_size` instead of `1`. `make_pipelined_rpc_effects/2` runs when an acknowledgement frees capacity or when a follower's match index advances, not only when a client writes. A lagging follower therefore catches up on a timer/ack loop.

### 14. Bounded acknowledgement bookkeeping

Represent each peer's in-flight ranges as a `:queue` of `{from, to}` and its acknowledged end indexes as a `MapSet` pruned to the in-flight window. Advance `match_index` by popping contiguous ranges; discard acknowledgements below the current match index. Memory is O(pipeline depth), and each reply does amortized O(1) work.

Alternative considered: keep the flat list and prune periodically. Rejected: `last_index in acked` and `advance_contiguous` are both O(n) and the list grows for the leader's lifetime.

### 15. Non-blocking transport with an envelope

`Transport.send_rpc/2` uses `:erlang.send(dest, msg, [:nosuspend])`, returns `:ok | {:error, :backpressure | :noconnection}`, and never sleeps. On failure the server records the peer's retry deadline and moves on. Reconnection is attempted on the next scheduled pass, not inline.

Messages carry `{:rpc, from_server_id, payload}` so `ServerRuntime` resolves the sender from the envelope. A local ETS cache (`pid -> server_id`) covers legacy senders; a message with neither is rejected.

### 16. Decouple apply progress from the commit path

Persist `last_applied` asynchronously: keep the in-memory value authoritative, write it to DETS on a timer or every N applied entries, and sync on clean shutdown. Recovery replays from the persisted value, so a small lag only costs replay, never correctness. Term and vote remain synchronous.

Alternative considered: keep a synchronous DETS sync per commit. Rejected: it doubles the disk sync count on the hot path and its value is not even used by recovery today.

### 17. Metrics initialized once

Initialize the ETS table at application/system start and cache the table name. `inc/set/observe` operate directly and do not call `init/0` or `:ets.whereis/1`. The lazy-create path remains only as a defensive fallback when the table is missing.

### 18. Segment-backed log with a bounded memory window

Wire `LogSegmentWriter` into `Log`:

- `Log` keeps `mem_from..mem_to` in the entries map and an ordered list of sealed segments.
- When `mem_to - mem_from` exceeds the window, seal the oldest range to a segment and drop it from memory.
- `Log.fetch/term/fold` check the memory window first, then locate the covering segment through a per-segment `{first, last}` index and read only that range.
- Compaction deletes segments entirely at or below the snapshot index.

Fix the segment index while wiring it: use a tuple/`:array` instead of `index ++`, build the sidecar with `IO.iodata_to_binary`, and keep a read fd rather than open/pread/close per entry.

Alternative considered: keep the whole log in memory and only add auto-compaction. Rejected: it does not satisfy the bounded-memory requirement and leaves the segment code as dead weight.

### 19. Automatic compaction thresholds

Track applied entries since the last snapshot, WAL bytes, and time since the last snapshot. A `state_timeout` checks the thresholds and emits the existing checkpoint effect. Compaction is skipped while a snapshot transfer is in progress and retried on the next check. Thresholds are configuration with a manual override.

### 20. End-to-end benchmark harness

Add scenarios that drive the real API:

- `command_single` and `command_cluster` (1/3/5 servers, sequential and concurrent),
- `follower_catchup` (idle leader, lagging follower),
- `wal_truncate` (prefix/suffix + restart verification),
- `snapshot_transfer` (>1 chunk, verify installed state).

Each scenario asserts its configured input (for example, `length(rpc.entries) == 100`) and reports entries/s, not RPCs/s. The comparison mode fails on a scenario whose measured input does not match its configuration.

### 21. Derive a unique uid per member

`start_cluster/4` currently derives one uid from the cluster name and assigns it to every server. Derive the uid from the cluster name **and** the server id (for example, a deterministic hash of both), or accept an explicit uid. On startup, detect an existing data directory whose uid belongs to a different server id and fail with an identity error instead of sharing the WAL. Existing single-node clusters with colliding directories need a fresh directory or an explicit uid; document the migration.

Alternative considered: require callers to pass a uid for every server. Rejected because `start_cluster/4` is the documented convenience path and must work without extra ceremony.

### 22. One commit-advance path with current-term and monotonic guards

Both the local command path and the AppendEntries-reply path call one `advance_commit/2` helper that:

- computes the candidate index from voter match indexes,
- requires the candidate entry's term to equal `current_term`,
- requires the candidate to be greater than the current `commit_index`,
- clamps a follower's commit to `min(leader_commit, last_index)`.

This removes the current split where `increment_commit_index/1` can move backwards and the reply path can commit a prior-term entry.

### 23. Voter-only elections and an authoritative local role

Gate `handle_follower(:election_timeout)` on the server's current role, add self to `votes_received` only when self is a voter, and include the leader in `voter_match_indexes` only when the leader is a voter. Remove the `membership` field fast path in `get_membership/1`; always consult the cluster map by id, and write the target's real uid (or stop matching on uid) in cluster-change entries so join, promote, and demote update the local role.

### 24. Leader step-down, check-quorum, and pre-vote tokens

The leader state routes incoming vote, append, heartbeat, and snapshot RPCs to the follower handler (or a shared adopt-term path) so a higher term always causes step-down. Track the last time a majority of voters acknowledged; the leader tick steps down when that exceeds the election timeout. Pre-vote stores its token in the election context, ignores replies with a stale token, and does not persist a self-vote until the real candidate phase begins.

### 25. Startup, join, pipeline, and counter corrections

- `RaftEx.start/1` calls `Application.ensure_all_started(:raft_ex)` before stopping the default system, and `stop_default/0` is a no-op when the supervisor is absent.
- `System.default_config/0` reads the application environment for WAL sync method, snapshot chunk size, pipeline depth, and batch size, and copies them into the per-server config.
- `join_cluster/5` validates the seed is in the supplied node set, propagates add-member errors, and returns success only after membership is confirmed.
- `ServerRuntime` handles network-up/network-down events by updating peer status and scheduling catch-up.
- A shared helper runs the RPC plug pipeline for every RPC type.
- `Counters.counters/2` and `incr_counter/3` are backed by the metrics store so `key_metrics/2` and `member_overview/2` report real values.

### 26. Campaign protection (H4)

A candidate or pre-vote server is demoted only by genuine leader traffic
(AppendEntries/Heartbeat/InstallSnapshot at its term), a strictly higher
term, or granting its vote to someone else. Polls, denied votes, local
queries, aux commands, and maintenance events keep the current state while
still executing their effects (e.g. answering the poll). Without this,
competitors' polls demote every candidate before it collects a quorum and
serial restarts livelock the cluster with ever-climbing terms.

The same round shares one pre-vote token end to end: `call_for_election/2`
stores it as `pre_vote_token`, and `Election.start_election(:pre_vote, ...)`
reuses the stored token instead of minting a second one, so the self-vote
and both vote-RPC batches correlate. A pre-vote timeout clears the stored
token first, forcing a fresh round whose stale replies are ignored.

## Risks / Trade-offs

- **uid derivation breaks existing single-node data directories** → Detect a uid collision at startup, fail with an actionable identity error, allow an explicit uid, and document the fresh-directory or explicit-uid migration.
- **Check-quorum can step down under a transient pause** → Base it on the election timeout, reset on any majority acknowledgement, and make the threshold configurable.
- **Failure-path behavior changes** → Tests that assert success on a broken WAL must change. Add explicit tests for error replies and document the change in `CHANGELOG.md`.
- **Atomic rewrite costs O(retained bytes)** → Conflicts and prefix truncations are rare; keep the rewrite off the common path and add a truncation benchmark. If it becomes hot, add an offset-based fast path that still renames.
- **Batching adds a scheduling hop** → The drain uses a zero-delay `:next_event`, so a single command waits at most one scheduler turn; measure with the end-to-end benchmark.
- **Segment wiring risks data loss** → Ship behind a configuration flag with the in-memory log as the default until the segment read path, recovery, and compaction tests pass. Dual-read (segment first, memory fallback) during migration.
- **Async `last_applied` loses a small amount of progress** → Only replay work is lost, never committed entries; sync on shutdown and on snapshot.
- **Snapshot membership metadata changes the format** → Version the metadata and fall back to `initial_nodes` for legacy snapshots; keep the payload encoding unchanged.
- **Large change** → Land in three phases, each with its own validation gate; do not start phase 2 until phase 1 safety tests are green.
- **Transport backpressure can delay replication** → Surface `replication_backpressure` and per-peer retry deadlines; never block the server.

## Migration Plan

1. **Phase 1 (safety)**: land term/vote, WAL truncation/recovery, snapshot transfer, runtime states, reply routing, election effects, delete lifecycle, config persistence. Existing data directories remain readable; no format migration is required except snapshot metadata (versioned, optional).
2. **Phase 2 (performance)**: land command batching, replication batching, ack tracking, transport, async apply progress, metrics, then measure against the recorded baseline. Keep the old per-command path behind a config flag until the benchmark gate passes.
3. **Phase 3 (storage/features)**: enable segments and automatic compaction behind config flags, then make them the default after recovery/compaction tests pass; add the end-to-end benchmark suite and update docs.
4. **Rollback**: each phase is a code revert. On-disk formats remain backward compatible; if a new snapshot version must be rolled back, the legacy fallback still reads it.

## Open Questions

- Default in-memory window size and compaction thresholds for a typical deployment; both are configurable, so the defaults can be tuned from benchmark data after phase 3.
- Whether segment files should be per-server or shared per system; the per-server layout is simpler and matches the existing WAL isolation, but a shared layout may reduce file count. This can be decided when wiring segments without changing the specs.
