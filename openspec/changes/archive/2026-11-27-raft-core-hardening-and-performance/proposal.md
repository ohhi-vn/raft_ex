# Proposal

## Why

A source-level review of RaftEx 0.6.0 found safety, liveness, and performance defects that the current specs and archived changes claim are already handled: servers started by `start_cluster/4` share one `uid` and therefore one WAL and data directory; a follower can clear its persisted vote on a same-term heartbeat (double-vote); prior-term entries can be committed by replica count alone; non-voters can campaign and count their own vote; a leader never steps down on a higher term; WAL truncation never reaches disk; multi-chunk snapshot transfer stalls after the first chunk; a WAL error wedges the leader forever; command replies and pipeline notifications are dropped; and the leader writes and fsyncs one entry at a time while replicating one entry per tick. The library is therefore neither safe to run in production nor able to demonstrate the throughput its benchmark documents claim. This change closes the gap between the documented contract and the implementation in one phased pass.

## What Changes

### Phase 1 — Safety and durability

- **Term/vote persistence**: only change `current_term`/`voted_for` when the incoming term is strictly greater; persist before replying; propagate persistence failures instead of acknowledging.
- **Unique server identity**: derive each member's `uid` from the cluster and server id so every server owns an independent WAL, metadata row, data directory, and registry entry.
- **Commit safety**: advance `commit_index` only for a current-term entry, never move it backwards, and ignore follower hints that would move `next_index` forward past the leader's log.
- **Leader step-down**: a leader that observes a higher term in any incoming RPC or reply SHALL adopt the term and become a follower; leaders SHALL process incoming vote, append, heartbeat, and snapshot RPCs instead of ignoring them.
- **Voter-only elections**: non-voters SHALL NOT campaign or count a self-vote, and a server's local membership SHALL reflect its current role after join, promotion, or demotion.
- **Check-quorum**: a leader that cannot reach a majority of voters within the election timeout SHALL step down instead of accepting writes indefinitely.
- **Pre-vote integrity**: pre-vote SHALL NOT persist a self-vote in the current term, and pre-vote replies SHALL be matched to their round token.
- **Durable WAL truncation**: prefix truncation SHALL actually remove bytes from disk (rewrite-to-temp + fsync + rename); suffix truncation SHALL be atomic; recovery SHALL verify CRCs, handle torn/empty records, and reject oversized payloads.
- **Atomic snapshots**: write snapshots to a temporary file, fsync file and directory, then rename; never delete the last valid snapshot on a failed transfer; include cluster membership in snapshot metadata and restore it.
- **Consistent snapshot install**: update the installed snapshot's term, last-written index/term, and snapshot state so later replies, applies, and snapshot sends use the correct snapshot term.
- **Complete RPC coverage**: run the configured RPC plug pipeline for every RPC type, not only AppendEntries, and handle local state queries instead of letting them time out.
- **Complete snapshot transfer**: handle middle/last chunks in the receiving state, strip the per-chunk CRC uniformly, bound transfers with a timeout, retry abandoned transfers, and reset per-peer send state.
- **No dead-end states**: `await_condition` re-evaluates the blocking condition with a timeout, retries, or steps down; unknown events are logged and replied to instead of silently ignored.
- **Fail-closed replication**: a follower that cannot durably write entries SHALL reject the AppendEntries request and SHALL NOT report success after truncating its log.
- **Command reply routing**: 4-tuple replies (`reply_from`) and pipeline notifications are delivered; commands never hang because an effect shape is unhandled.
- **Election effects**: `{:update_term, t}` and `{:become, :follower}` are applied by the runtime; `TimeoutNow` is accepted only from the current leader and persists the new term first.
- **Cluster lifecycle**: `delete_cluster` terminates cleanly instead of entering a nonexistent state and crash-looping; unknown or removed peers cannot crash or re-insert themselves.
- **Recovery that works**: persist server configuration at start so registered-server recovery can actually restart servers; remove the legacy `:ra` environment fallback; start the application before stopping the default system; honor the documented startup parameters (WAL sync method, snapshot chunk size, pipeline depth, batch size).
- **BREAKING (correctness)**: durable-write failures now return errors instead of success, and startup no longer reads `:ra` configuration.

### Phase 2 — Replication and write-path performance

- **Server-level batching**: accumulate pending commands into one WAL write and one fsync per batch, then send one AppendEntries batch per peer; a single sequential client benefits too.
- **Real replication batching**: size AppendEntries with the configured maximum batch size and available pipeline slots; trigger pipelining on acknowledgement so a lagging follower catches up without new client traffic.
- **Bounded acknowledgement tracking**: keep in-flight and acknowledged ranges bounded and advance `match_index` in amortized O(1) instead of growing lists and O(n) scans.
- **Non-blocking transport**: never sleep or block the consensus process on a partitioned peer; use non-suspending sends, per-peer isolation, and scheduled retries.
- **Cheap sender identity**: carry the sender id in the RPC envelope or resolve it from a local cache instead of a remote `:erpc.call` per message.
- **Decoupled apply progress**: persist `last_applied` asynchronously/periodically or on the WAL path instead of a second synchronous DETS sync per commit; use it to shorten replay.
- **Hot-path hygiene**: initialize metrics once, remove the mailbox copy in the WAL flush check, avoid per-entry map rebuilding on apply, and stop re-reading/re-reversing the same batch per peer.
- **Bounded storage**: rotate/truncate WAL segments and trigger compaction automatically from configurable entry/byte/interval thresholds.

### Phase 3 — Feature completeness and evidence

- **Automatic compaction and retention**: checkpoint on entry count, WAL bytes, or time, with an operator override; compact log and WAL through the snapshot index.
- **Segment-backed log storage**: serve cold entries from sealed segments so the in-memory log stays bounded, with a documented fallback when segments are absent.
- **Join and discovery correctness**: honor the configured seed node, report join success only after membership is confirmed, propagate add-member errors, and handle network-up/network-down events.
- **Per-server counters**: back `key_metrics/2` and `member_overview/2` with real counter values instead of stubs.
- **End-to-end benchmarks**: measure client→leader→WAL→replication→commit→reply (single node and 3/5 nodes), follower catch-up, WAL truncation, and snapshot transfer; fix the mislabeled replication scenario and gate comparisons.
- **Documentation truthfulness**: update the feature-status matrix, performance docs, and troubleshooting guidance so claimed and verified behavior match.

## Capabilities

### New Capabilities

- `command-submission`: command submission, batching, reply routing (including `reply_from`), pipeline notifications, and failure contracts for `process_command`/`pipeline_command`.
- `log-storage`: bounded logical log storage, segment-backed cold-entry reads, and retention of entries after compaction.

### Modified Capabilities

- `leader-election`: strict term/vote persistence, election-effect application, `TimeoutNow` authorization, and leadership-transfer durability.
- `log-replication`: real batch sizing, bounded acknowledgement tracking, catch-up without client traffic, fail-closed follower writes, and non-blocking peer communication.
- `write-ahead-log`: durable prefix truncation, atomic suffix rewrite, CRC-verified recovery, configurable rotation, and error propagation.
- `snapshots`: atomic snapshot install, complete multi-chunk transfer, transfer timeouts/retries, membership in snapshot metadata, and automatic compaction triggers.
- `raft-queries`: bounded consistent-query waiting with timeouts, efficient quorum evaluation, and reply on every outcome.
- `raft-startup`: no `:ra` fallback, persisted server configuration, and recovery that restarts registered servers.
- `cluster-membership`: membership restored from snapshots, safe handling of replies from removed peers, and preserved membership across restart.
- `distributed-clusters`: non-blocking cross-host sends, fast failure on partition, and reconnect without stalling consensus.
- `observability`: correct replication-lag metrics, per-server counters, and metric initialization outside the hot path.
- `plug-pipeline`: run the configured RPC pipeline for every RPC type, not only AppendEntries.
- `performance-benchmarks`: end-to-end command, catch-up, truncation, and snapshot benchmarks with accurate scenario accounting and regression gating.

## Impact

- **Core code**: `lib/domain/server/{server,rpc_handler,election,cluster,effects}.ex`, `lib/app/{server_runtime,cluster,recovery}.ex`, `lib/domain/{pipeline,conn}.ex`, `lib/raft_ex.ex`.
- **Persistence**: `lib/infra/persistence/{log,log_wal,log_snapshot,log_meta,log_segment_writer}.ex`, `lib/infra/filesystem.ex`.
- **Transport/observability**: `lib/infra/network/{transport,distribution}.ex`, `lib/infra/observability/metrics.ex`, `lib/infra/config/env.ex`.
- **Storage compatibility**: WAL and snapshot on-disk formats remain readable; truncation now physically removes data, and snapshots gain membership metadata (legacy snapshots fall back to `initial_nodes`).
- **APIs**: no public function removals. Failure paths return errors where they previously returned success; `:ra` environment fallback is removed; `start_cluster/4` assigns distinct uids, so existing single-node clusters with colliding data directories need a fresh data directory or an explicit uid. **BREAKING** for callers relying on those behaviors.
- **Safety behavior**: current-term-only commit advancement, monotonic commit index, voter-only elections, leader step-down on higher terms, check-quorum, and pre-vote token validation.
- **Tests**: add crash/restart, torn-write, ENOSPC, multi-chunk (>64 KiB), partition, duplicate/reordered RPC, concurrent membership, and end-to-end throughput coverage; replace no-op recovery assertions.
- **Dependencies**: none required; benchmark harness stays dependency-free.
- **Docs**: `README.md`, `docs/performance.md`, `docs/persistence-invariants.md`, `guides/*.md`, `CHANGELOG.md`.
