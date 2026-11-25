# Tasks

## 1. Peer state and voter helpers

- [x] 1.1 Add `voter_status: %{membership: :voter, uid: nil}` to `Cluster.new_peer/0`, default missing fields in `Cluster.merge_peer/1`, and verify `mix test test/domain/server/election_test.exs test/app/cluster_test.exs` passes.
- [x] 1.2 Add `Cluster.voters/1`, `Cluster.voter_count/1`, and role-aware `Cluster.get_membership/4`, then verify new unit tests in `test/domain/server/cluster_test.exs` cover voter, promotable, non-voter, and default-role peers.
- [x] 1.3 Replace ad-hoc voter counting in `Election.count_voters/1`, `Election.build_election_rpcs/4`, and commit-quorum code with the `Cluster` helpers, and verify a test proves a non-voter is neither counted nor sent a vote request.
- [x] 1.4 Round-trip persisted cluster configuration without `voter_status` and verify loading it defaults every unknown member to `:voter` in `test/app/recovery_test.exs`.

## 2. Startup and configuration identity

- [x] 2.1 Change `RaftEx.start/1` and `RaftEx.start_in/1` to stop only the default system, write parameters to the `:raft_ex` application environment, call `Application.ensure_all_started(:raft_ex)`, start the default system, and return `:ok | {:error, reason}`, then verify `test/raft_ex_test.exs` asserts no `:ra` application is stopped or started.
- [x] 2.2 Make `RaftEx.Infra.Config.Env.data_dir/0` read `:raft_ex` `:data_dir` first and fall back to `:ra` for one release, and verify `test/infra/config_env_filesystem_test.exs` covers both precedence and fallback.
- [x] 2.3 Return real errors from `RaftEx.App.System.start_default/0` and from repeated startup, and verify a test with an unwritable data directory receives `{:error, _}` instead of a silent success.
- [x] 2.4 Document the `:raft_ex` environment change and one-release `:ra` fallback in `CHANGELOG.md`, and verify the migration note names both environment keys.

## 3. RPC dispatch and durable term/vote

- [x] 3.1 Replace the runtime's per-message clauses with one event clause per state that unwraps `{from_pid, msg}` and dispatches to the domain layer, and verify `test/app/server_runtime_callbacks_test.exs` covers every RPC struct in `RaftEx.Domain.Types`.
- [x] 3.2 Add `Server.adopt_term/3` returning `{:store_meta, key, value}` effects and execute `store_meta` effects with `LogMeta.store_sync/4` in `ServerRuntime`, then verify a unit test shows a term change and vote grant are persisted before any reply effect runs.
- [x] 3.3 Route `RpcHandler.update_term/2` and `Election.handle_vote_request/2` through `Server.adopt_term/3`, and verify `test/domain/server/rpc_handler_test.exs` asserts `voted_for` is unchanged and durable when a vote is denied.
- [x] 3.4 Restore `current_term`, `voted_for`, and `last_applied` from `LogMeta` in `Server.init/1`, and verify `test/app/recovery_test.exs` proves a restarted server reports the persisted term and does not vote twice in it.

## 4. Elections

- [x] 4.1 Dispatch `PreVoteRpc`, `RequestVoteRpc`, `PreVoteResult`, and `RequestVoteResult` in the follower, pre-vote, and candidate state functions, and verify `test/domain/server/server_test.exs` covers grant, deny, stale-term, and out-of-date-log cases.
- [x] 4.2 Reset the election timer on accepted `AppendEntriesRpc`, `HeartbeatRpc`, and `InstallSnapshotRpc` via a `{:reset_election_timeout}` effect, and verify a test shows no election starts while leader traffic arrives before the timeout.
- [x] 4.3 Choose election timeouts randomly between `election_timeout_min` and `election_timeout_max` with the existing fallback, and verify `test/app/server_runtime_test.exs` asserts the value stays within the configured range.
- [x] 4.4 Persist the candidate's incremented term and self-vote before sending vote requests, and verify a candidate-election test observes the persisted term and vote at send time.
- [x] 4.5 Append a leader no-op entry on `handle_state_enter(:leader, state)`, keep `cluster_change_permitted` false until it applies, and verify tests prove prior-term entries commit and membership changes are rejected before the no-op commits.
- [x] 4.6 Emit a leader no-op with `{:noop, %{}, machine_version}` and verify `test/integration/machine_effects_test.exs` still passes so user commands and effects are unaffected.

## 5. Log replication

- [x] 5.1 Reject `AppendEntriesRpc` when the previous index/term do not match and reply with a lower next-index hint, and verify `test/domain/server/rpc_handler_test.exs` covers missing index, conflicting term, and matching prefix.
- [x] 5.2 On rejection, reduce the follower's next index from the hint or by one and immediately resend instead of waiting for the next tick, and verify a leader test observes a second AppendEntries without an intervening heartbeat tick.
- [x] 5.3 Add `LogWal.truncate_from/3` and make `Log.truncate/2` remove the conflicting suffix from both the in-memory log and the WAL, then verify a restart test does not recover truncated entries.
- [x] 5.4 Compute commit advancement from voter match indexes only, require a current-term entry, and clamp follower application to `min(leader_commit, last_index)`, then verify tests cover majority commit, ignored non-voter ack, prior-term entry, and clamped application.
- [x] 5.5 Include the leader commit index in heartbeats and apply newly committed entries on the follower, and verify an idle-follower test applies entries from a heartbeat alone.
- [x] 5.6 Keep `last_applied` durable after follower application and verify `test/app/recovery_test.exs` resumes from the recorded index after restart.

## 6. Queries and auxiliary commands

- [x] 6.1 Add `{:local_query, fun, opts}` and `{:leader_call, {:local_query, fun, opts}}` clauses to every server state that answer through `Machine.query/4` at `last_applied` and reply `{:ok, result, id}`, and verify follower and leader query tests in `test/integration/facade_integration_test.exs` no longer accept `{:timeout, _}`.
- [x] 6.2 Return a no-leader error for leader queries when no leader is known and redirect from followers to the known leader, and verify both paths with tests.
- [x] 6.3 Implement consistent-query admission on the leader: reject or queue while `cluster_change_permitted` is false, allocate a query index, capture `read_ci = commit_index`, send heartbeat probes, and track waiting queries.
- [x] 6.4 Evaluate heartbeat acknowledgements to count voters with `query_index >= qi`, release queries on a majority, hold them until `last_applied >= read_ci`, and remove the crashing assert; verify tests cover majority confirmation, no quorum, and a query waiting for apply.
- [x] 6.5 Run pending consistent queries after applying a cluster change or no-op, and verify a test shows a query queued behind an uncommitted cluster change is answered after it commits.
- [x] 6.6 Add `{:aux_command, cmd}` and `{:aux_command, cmd, opts}` clauses for calls and casts in all states, wire `handle_aux/5` and `handle_aux/6` with the correct machine state, and verify call, cast, missing-handler, and `consistent_aux/3` tests pass.
- [x] 6.7 Document query consistency levels and aux command behaviour in `README.md`, and verify each documented call appears in a passing test.

## 7. Leadership transfer

- [x] 7.1 Add `Types.TimeoutNowRpc` with `term` and `leader_id` and verify `test/domain/types_test.exs` covers construction and defaults.
- [x] 7.2 Implement leader-side `{:transfer_leadership, target}` validation (different, known, caught-up voter), send `TimeoutNowRpc`, record the transfer target, and reject new commands during the hand-off; verify invalid-target and in-progress tests pass.
- [x] 7.3 Start an immediate candidate election on `TimeoutNowRpc` without pre-vote, clear the transfer target when a higher term is adopted, and verify an old-leader-steps-down test passes.
- [x] 7.4 Add a timeout that clears a stalled transfer so the old leader resumes accepting commands, and verify a test covers a target that never acts.

## 8. Snapshots and log lifecycle

- [x] 8.1 Implement `Log.update_release_cursor/5` to record a pending release cursor and re-evaluate it after log writes and applies, and verify a unit test shows the cursor is checkpointed only when the index is written and no snapshot send is in flight.
- [x] 8.2 Implement `Log.checkpoint/5` to write `index`, `term`, `cluster`, `machine_version`, and machine state through the snapshot module, promote the snapshot, update `current_snapshot`/`snapshot_state`, and verify `test/infra/persistence/log_test.exs` covers a checkpoint at an applied index.
- [x] 8.3 Handle `{:release_cursor, idx, mac_state}` and `{:checkpoint, idx, mac_state}` effects in every server state, and verify `test/integration/machine_effects_test.exs` shows a snapshot is created from a machine release cursor.
- [x] 8.4 Truncate memory and WAL entries through the snapshot index while retaining later entries, and verify a restart test restores from the snapshot and replays only later entries.
- [x] 8.5 Route `trigger_compaction/1` to a checkpoint at `last_applied` and write a final checkpoint in `Server.terminate/2`, and verify both paths with tests.
- [x] 8.6 Load the latest valid snapshot on startup and report a checksum mismatch without installing partial state, and verify `test/infra/persistence/log_snapshot_test.exs` covers valid and corrupt snapshots.
- [x] 8.7 Track per-peer snapshot sends with `%{meta, read_state, offset}` and stream first/middle/last/full chunks from disk using `snapshot_chunk_size`, and verify a multi-chunk and a single-chunk transfer test both complete.
- [x] 8.8 Install a received snapshot on the follower: validate the checksum, call the machine snapshot-installed callback, update applied/commit indexes, truncate the WAL, and reply `InstallSnapshotResult`; verify success and corruption tests.
- [x] 8.9 On `InstallSnapshotResult`, advance the peer match index and resume AppendEntries at `snapshot_index + 1`, and verify the follower counts toward commit again after installation.
- [x] 8.10 Abort transfers on a higher term, send error, or superseded snapshot, delete partial accept files, and verify a stale-term test stops chunk sending and steps the leader down.

## 9. Cluster membership

- [x] 9.1 Extend the join payload to carry `{server_id, membership}` while keeping bare server ids as `:voter`, and verify a default join and an explicit non-voter join both commit.
- [x] 9.2 Reject duplicate joins, unknown removals, and any membership change while `cluster_change_permitted` is false, and verify each error path in `test/app/cluster_test.exs`.
- [x] 9.3 Add `$ra_promote` and `$ra_demote` cluster changes with catch-up, non-voter, voter, and last-voter validation, and verify caught-up promotion, lagging rejection, demotion, and last-voter rejection tests pass.
- [x] 9.4 Expose roles through `members_info/2`, store configured members as `initial_members`, and verify `members/2`, `members_info/2`, and `initial_members/2` return the expected values.
- [x] 9.5 Update `RaftEx.add_member/4`, `promote_member/3`, and `demote_member/3` documentation and `README.md` membership examples, and verify each documented call appears in a passing test.

## 10. Three-server integration coverage

- [x] 10.1 Add a three-server test helper that starts `{:s1, node()}, {:s2, node()}, {:s3, node()}` under one system with isolated data directories, and verify the helper elects exactly one leader within the configured timeout.
- [x] 10.2 Add an election test that kills the leader and verifies a remaining voter becomes leader in a higher term without splitting the cluster.
- [x] 10.3 Add a replication test that commits a command on the leader and verifies all followers apply it at the same index, including after one follower restarts.
- [x] 10.4 Add query tests against the three-server cluster for local, leader, and consistent reads, including a consistent read while a follower is stopped.
- [x] 10.5 Add leadership-transfer, snapshot-catch-up, and membership-role tests against the three-server cluster, and verify each exercise returns without a timeout.
- [x] 10.6 Update `README.md` feature status and the test limitations section, and verify every feature marked complete is covered by a named passing test.

## 11. Integration and quality gate

- [x] 11.1 Run `mix format --check-formatted` and verify it exits successfully.
- [x] 11.2 Run `mix credo --strict` and resolve or explicitly justify every finding in the change.
- [x] 11.3 Run `mix dialyzer` and verify no new warnings come from the changed modules. Justified: remaining warnings are pre-existing project-wide design/type-annotation findings; change-specific unused-variable warning in `Log.install_snapshot/4` was fixed.
- [x] 11.4 Run the full `mix test` suite three times with different seeds and verify no failures, including the previously flaky test.
- [x] 11.5 Update `CHANGELOG.md` with the completed consensus, snapshot, membership, query, and startup changes, and verify the entry names every capability in this change.

## Workflow follow-up

- Archive the change after review and after the full quality gate passes.
- Verify the archived change with `openspec validate --all`.
