# Design

## Context

RaftEx separates pure protocol logic (`RaftEx.Domain.Server` and its helpers) from OTP orchestration (`RaftEx.App.ServerRuntime`). Domain handlers return effects that the runtime executes. Persistence is layered: `Log` keeps an in-memory entry map plus an ETS mem table, `LogWal` stores durable entries per server uid, `LogMeta` stores term/vote/applied state in DETS, and `LogSnapshot` stores snapshot files.

The constraints that shape this design:

- Existing single-server tests (553 passing, one flaky) must keep passing.
- The domain layer already performs some persistence directly (`LogMeta` calls in `Server`), so this change does not attempt a full purity refactor.
- Peer state is a plain map. Several modules already expect a `voter_status` key that `Cluster.new_peer/0` never creates.
- Snapshot primitives exist in `LogSnapshot`, but nothing creates snapshots and the transfer format does not match the receiver.
- Multi-server clusters can be exercised on one BEAM node because the local send path is the same cast shape as the remote path. Real distribution remains a separate concern.

See `proposal.md` for motivation and the six delta specs for the behaviour contracts.

## Goals / Non-Goals

**Goals:**

- Make a three-server cluster elect one leader, replicate and commit entries, and survive a follower restart.
- Make local, leader, consistent queries and aux commands return instead of timing out.
- Create, transfer, install, and recover snapshots, and bound log growth.
- Add voter/promotable/non-voter roles with safe join, leave, promote, and demote.
- Make startup use `:raft_ex` and report failures.
- Cover each behaviour with unit or three-server integration tests.

**Non-Goals:**

- Joint-consensus membership changes. This change keeps Raft's one-change-at-a-time model plus a catch-up requirement before promotion.
- Real multi-host network partition testing. Tests use multiple servers on one node.
- Performance work (pipelining, batching, `gen_batch_server`), metrics completion, or machine-version upgrades beyond the existing no-op path.
- Changing the public command/query return shapes except where they are currently broken.

## Decisions

### 1. Add `voter_status` to peer state

**Decision:** `Cluster.new_peer/0` returns `%{next_index: 1, match_index: 0, commit_index_sent: 0, query_index: 0, status: :normal, voter_status: %{membership: :voter, uid: nil}}`. `Cluster.get_membership/4` matches on the peer's `voter_status.membership` and falls back to the passed default. Add `Cluster.voters/1` and use it in election RPC building and commit quorum calculation.

**Rationale:** `Election.count_voters/1`, `Election.build_election_rpcs/4`, `Cluster.voter_match_indexes/3`, and `Cluster.get_membership/4` already read this shape. Filling in the missing field is smaller and safer than rewriting every caller around a separate voters set.

**Alternatives considered:** Keep a separate `:voters` MapSet in server state. Rejected because it duplicates membership data and would need to be kept in sync with every cluster-change entry and config file.

**Compatibility:** Existing persisted cluster configs and test fixtures lack `voter_status`; `Cluster.merge_peer/1` supplies the default. The `uid` field is left `nil` for peers because peer uids are not currently stored; matching therefore ignores uid.

### 2. Central RPC dispatch and effect-driven durability

**Decision:** Expand `RaftEx.Domain.Server.handle_follower/2` and `handle_leader/2` to match every `RaftEx.Domain.Types` RPC and reply struct. The runtime keeps a single generic event clause per state that unwraps `{from_pid, msg}` and dispatches to the domain layer; the dead `{:cast, {from, reply}}, :vote_reply` clauses are removed. Vote-result correlation uses `state.rpc_from` (already derived from the sender pid) instead of the `{:election_peer, pid}` process dictionary.

Term changes go through `Server.adopt_term/3`, which updates `current_term`/`voted_for` and returns `{:store_meta, key, value}` effects. The runtime executes those effects with `LogMeta.store_sync/4`. Candidate entry uses the same helper so the term and self-vote are durable before vote requests are sent.

**Rationale:** The current runtime ignores `{:store_meta, ...}` effects and never dispatches vote RPCs or replies. Centralizing term adoption prevents one path (RPC handler) from bypassing persistence while another (`Election`) persists only part of the state.

**Alternatives considered:** Have each domain handler call `LogMeta` directly. Rejected because it hides durability in pure protocol code and already caused the current inconsistency. Keeping the process dictionary was rejected because it is lost on restart and makes reply attribution fragile.

### 3. Election timeouts live in the runtime, accepted leader traffic resets them

**Decision:** `ServerRuntime.set_election_timeout/2` chooses a random value between `system_config.election_timeout_min` and `election_timeout_max` (falling back to the current `broadcast_time` calculation when absent). Accepted `AppendEntriesRpc`, `HeartbeatRpc`, and `InstallSnapshotRpc` handlers emit a `{:reset_election_timeout}` effect; the runtime converts it to a `:state_timeout` action in follower-like states.

**Rationale:** A follower currently sets one timeout on entry and never resets it, so it starts elections while a healthy leader is still heartbeating. Using the configured min/max also removes the accidental tie to `broadcast_time`.

**Alternatives considered:** Ignore expired timeouts when leader traffic was recently seen. Rejected because it keeps a stale timer running and makes tests timing-dependent.

### 4. Leader appends a no-op on election

**Decision:** `Server.handle_state_enter(:leader, state)` appends `{:noop, %{}, machine_version}` at `next_index`, resets `cluster_change_permitted` to `false`, and returns the replication effects. Reuse the existing `apply_entry` no-op clause to set `cluster_change_permitted: true` once the entry applies.

**Rationale:** Raft leaders cannot commit entries from earlier terms until a current-term entry is replicated, and the existing code never creates one. Consequently a new leader can leave prior entries uncommitted, and `cluster_change_permitted` stays `false` forever, rejecting every join/leave. The no-op fixes both with one entry.

**Alternatives considered:** Append a user command or a cluster-change entry. Rejected because it would change application state or membership semantics.

### 5. Reads: one dispatch path, three consistency levels

**Decision:** Add a shared `Server.query/3` helper and small state-function clauses in `ServerRuntime` for `{:local_query, fun, opts}`, `{:leader_call, {:local_query, fun, opts}}`, and `{:consistent_query, fun}`. Local and leader reads call `Machine.query/4` at `last_applied` and reply `{:ok, result, id}`. Consistent reads run on the leader only:

1. If `cluster_change_permitted` is false, append the query to `pending_consistent_queries`.
2. Otherwise allocate the next `query_index`, capture `read_ci = commit_index`, send `HeartbeatRpc` with that query index to peers that have a lower query index, and enqueue `{query_index, query_ref}`.
3. On `HeartbeatReply`, update the peer's `query_index` and evaluate every waiting query: count voters (including self) with `query_index >= qi`. On a majority, remove the query from the queue. If `last_applied >= read_ci`, run it and reply; otherwise move it to a pending-apply list processed after the next apply.
4. On applying a cluster change or no-op, run `process_pending_consistent_queries/2` so queued queries are released.

`consistent_aux` reuses the same queue with `{:aux, from, cmd, read_ci}` and dispatches to the auxiliary handler instead of `Machine.query/4`.

**Rationale:** The scaffolding for query indexes, waiting heartbeats, and pending consistent queries already exists; only the leader clauses and quorum evaluation are missing. Reusing it avoids a second mechanism.

**Alternatives considered:** Append a read barrier entry to the log. Rejected because it writes to disk for every read and Ra already models read-index queries with heartbeats.

**Apply-lag handling:** The current `apply_consistent_queries_effects/2` asserts `last_applied >= read_ci` and would crash under load. The design removes the assert and keeps the query queued until application catches up.

### 6. Auxiliary commands are first-class calls in every state

**Decision:** Add `{:aux_command, cmd}` and `{:aux_command, cmd, opts}` clauses to leader, follower, candidate, and pre-vote state functions. Calls go through `Server.handle_aux/4` and reply with `{:reply, from, reply}` effects; casts run the handler without a reply. `RaftEx.aux_command/3` and `cast_aux_command/2` keep their signatures. `handle_aux/5` receives the aux state and server state; `handle_aux/6` receives the aux state, log, and machine state, matching the behaviour callback.

**Rationale:** The current `RaftEx.aux_command/3` sends a `:gen_statem.call` that no state function matches, so every call times out. The domain implementation also passes the full server state where the 6-arity callback expects the machine state.

**Alternatives considered:** Route aux commands through the command log. Rejected because aux state is not replicated and is explicitly local.

### 7. Leadership transfer uses a TimeoutNow RPC

**Decision:** Add `Types.TimeoutNowRpc` with `term` and `leader_id`. `Server.handle_leader({:transfer_leadership, target})` validates that the target is a different, caught-up voter and sends the RPC, records `transfer_leadership: target`, and replies `{:ok, target}`. While the field is set, new commands reply `{:error, :leadership_transfer}`. A follower receiving `TimeoutNowRpc` with a term at least its own starts a candidate election immediately, bypassing pre-vote. The old leader clears the field and steps down when it adopts a higher term; a short state timeout clears it if the target does not act.

**Rationale:** Pre-vote would delay the hand-off and a target that cannot win would leave the cluster leaderless. Bypassing pre-vote is the standard Raft leadership-transfer mechanism. Rejecting new commands during the short hand-off avoids committing entries the new leader may not have.

**Alternatives considered:** Old leader steps down first and lets a normal election run. Rejected because it creates a needless leaderless window and may elect a different server.

### 8. Snapshots are checkpointed from machine release cursors

**Decision:** Implement `Log.checkpoint/5` and `Log.update_release_cursor/5`:

- `update_release_cursor(index, cluster, {mac_ver, mac_mod}, mac_state, log)` records `pending_release_cursor: {index, mac_state, [{:written, index}, :no_snapshot_sends]}`. `Server` re-evaluates the conditions after log writes and applies; when satisfied it calls `Log.checkpoint/5`.
- `checkpoint/5` builds `%{index: index, term: term_at(index), cluster: cluster, machine_version: mac_ver}`, writes it with the machine's snapshot module to `<server_data_dir>/snapshot/snapshot.dat`, then promotes it: set `current_snapshot`, `snapshot_state`, clear in-memory entries through the index, recompute `range`/`next_index`/`last_written_index_term`, and call `LogWal.truncate(wal_name, uid, index)`.
- `Server` handles `{:release_cursor, idx, mac_state}` and `{:checkpoint, idx, mac_state}` effects in every state, not only leader.
- `trigger_compaction/1` casts `{:ra_log_event, :major_compaction}`; the server checkpoints at `last_applied` when it is ahead of the current snapshot and no transfer is in flight.
- `Server.terminate/2` writes a final checkpoint when `last_applied` is ahead of the snapshot, replacing the current no-op.

**Rationale:** Machines already emit `{:release_cursor, Index, MacState}` to signal that their state at an index can be released. Using that signal avoids background scanning and keeps snapshot contents consistent with an applied index.

**Alternatives considered:** Snapshot on a timer. Rejected because it can capture an index that is not yet applied or force snapshots with no state change.

### 9. Snapshot transfer streams from disk and tracks state per peer

**Decision:** Add a `snapshot_sends` map to server state keyed by peer id, each holding `%{meta: meta, read_state: read_state, offset: offset}`. When a leader needs a snapshot (`make_rpc_effect/5` sees `prev_idx < first retained index`), it emits `{:send_snapshot, peer_id}`. The runtime opens the snapshot with `Snapshot.begin_read/2`, reads `snapshot_chunk_size` bytes, and sends:

- `chunk_state: :full` when the first read returns `:last`
- `chunk_state: :first` for the first of several chunks, then `:middle`, then `:last`

The follower keeps `snapshot_accept` in server state, calls `Snapshot.begin_accept/2`, `accept_chunk/2`, and `complete_accept/2`, validates with `Snapshot.recover/1`, installs through `Log.install_snapshot/4`, calls `Machine.snapshot_installed/5`, sets `last_applied` and `commit_index` to at least the snapshot index, truncates the WAL, and replies `InstallSnapshotResult`. On `InstallSnapshotResult`, the leader advances the peer's `match_index`, removes the send state, and resumes AppendEntries at `snapshot_index + 1`. A higher term, a send error, or a newer snapshot cancels the transfer and releases the read handle.

**Rationale:** The current sender serializes the whole snapshot into memory and only emits `:more`/`:last`; the receiver expects `:first`/`:middle`/`:last`/`:full`, so a transfer cannot complete. Streaming from disk with an explicit per-peer state also bounds memory and lets a transfer resume or abort cleanly.

**Alternatives considered:** Keep `term_to_binary(snapshot_state)` and fix only the chunk labels. Rejected because it duplicates the snapshot bytes in the heap and cannot serve snapshots larger than memory.

**Failure handling:** A partial accept file is deleted on abort. `LogSnapshot` gains an `abort_accept/1` helper (or the runtime closes the fd and removes the file) so failed transfers do not leave a usable-looking snapshot.

### 10. Conflict truncation reaches the WAL

**Decision:** Add `LogWal.truncate_from(wal_name, uid, from_index)` that rewrites the server's WAL file without entries at or after `from_index` (preserving the format and fsyncing). `Log.truncate/2` calls it and then updates the in-memory map. `Log.install_snapshot/4` uses `LogWal.truncate/4` for the prefix and updates `last_written_index_term` to at least `{snap_idx, snap_term}`.

**Rationale:** The current `Log.truncate/2` only edits the in-memory map. A restart replays the conflicting suffix from the WAL, violating the Log Matching Property. Prefix truncation after a snapshot has the same issue if the WAL is left untouched.

**Alternatives considered:** Rewrite the whole WAL from the in-memory map on every truncation. Rejected because truncations happen on the replication hot path; a targeted suffix rewrite is cheaper and keeps the existing format.

### 11. Membership roles are carried in cluster-change entries

**Decision:** Extend the join command payload to `{:"$ra_join", from, {server_id, membership}, reply_mode}` while keeping a clause for the bare server id (`:voter`). Add `$ra_promote` and `$ra_demote` commands that build a new cluster map and append it through the existing `$ra_cluster_change` entry. Validation:

- Join: reject if already present; default role `:voter`.
- Remove: reject unknown members; allow leader self-removal, after which it steps down.
- Promote: target must be a non-voter/promotable and have `match_index >= commit_index`.
- Demote: target must be a voter; reject if it is the only voter.
- All changes: require `cluster_change_permitted`; one uncommitted change at a time.

`RaftEx.add_member/4`, `promote_member/3`, and `demote_member/3` expose the roles. `Server.init/1` stores the configured initial member list under `:initial_members` so `initial_members/2` does not just echo the current cluster.

**Rationale:** Carrying the role in the same replicated cluster-change entry keeps membership and role changes atomic. The catch-up requirement prevents adding a voter that would immediately be counted but cannot acknowledge entries.

**Alternatives considered:** Joint consensus. Rejected as a non-goal; the one-change-at-a-time plus catch-up rule is the existing Ra-style model.

### 12. Startup uses `:raft_ex` and returns real results

**Decision:** `RaftEx.start/1` stops the default system if present, writes parameters to the `:raft_ex` application environment, calls `Application.ensure_all_started(:raft_ex)`, then `RaftEx.App.System.start_default/0`, and returns `:ok` or `{:error, reason}`. `RaftEx.start_in/1` delegates. `Config.Env.data_dir/0` reads `:raft_ex` `:data_dir` first, then falls back to `:ra` for one release, and documents the change.

**Rationale:** The current code stops, loads, and starts `:ra`, an application this library does not own, and ignores the failure. `start_in/1` only works by side effect. The `:raft_ex` app already owns the root supervisor.

**Alternatives considered:** Keep the `:ra` names for compatibility with the port. Rejected because it can stop an unrelated Ra installation and makes configuration confusing.

## Risks / Trade-offs

- **Three servers on one BEAM do not exercise network partitions or serialization** → Keep transport unit tests, add a real multi-node test only as a follow-up, and state the limitation in the README.
- **Single-step membership changes can reduce availability** → Require catch-up before promotion, reject changes during a pending change, and never allow the last voter to be demoted.
- **Synchronous term/vote persistence adds latency to elections** → Use `store_sync` only for term and vote writes; keep `last_applied` checkpointing as it is.
- **Snapshot transfers hold file descriptors** → Track a read/accept handle per peer and add abort paths for term changes, disconnects, and superseded snapshots.
- **WAL suffix rewrite is O(file size)** → Only perform it on a conflicting AppendEntries, fsync once, and keep the existing prefix untouched.
- **Snapshotting on every release cursor can be expensive** → Gate checkpoints on the configured interval and the `:no_snapshot_sends` condition; `trigger_compaction` is explicit.
- **Queries queued behind a cluster change may outlive the caller** → Callers still get their timeout; the queue entry is removed when the query is answered, and a periodic prune can drop entries past their deadline.
- **Removing the `:ra` environment fallback is a config break** → Keep the fallback for one release and document the migration in CHANGELOG.
- **The one flaky test in the baseline may become more visible** → Run the suite with a fixed seed in CI and fix or quarantine the flake separately.

## Migration Plan

1. Ship the peer-state default first; old cluster-config files and fixtures load with `voter_status` defaulted to `:voter`.
2. Ship startup and environment changes with `:raft_ex` taking precedence and `:ra` as a one-release fallback.
3. Snapshot directories are new; absence is handled by the existing recovery path. New snapshots use the existing `RASN` format, so an older build can still read a snapshot file, but older builds do not compact the WAL; do not roll back after a compaction without taking a backup.
4. Rollback: revert the code; WAL files truncated by snapshots remain truncated, so recovery relies on the snapshot. Keep the snapshot file when rolling back.
5. Run `mix format`, `mix credo --strict`, `mix dialyzer`, and the full test suite before release; update README and CHANGELOG.

## Open Questions

None. The remaining choices (chunk size default, checkpoint interval default) are configuration values that can be tuned during implementation without changing the specs or the task breakdown.
