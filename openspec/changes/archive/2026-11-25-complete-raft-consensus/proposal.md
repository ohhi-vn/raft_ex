# Proposal

## Why

RaftEx is only reliable as a single-server cluster today. The election, query, snapshot, and membership code exists but key RPC paths are never dispatched, voter roles are absent from peer state, and snapshots are never produced. A three-server cluster therefore self-elects without votes, consistent reads time out, lagging followers cannot be recovered from a snapshot, and membership roles cannot be distinguished. These are correctness and liveness gaps in the Raft protocol, not cosmetic missing features.

## What Changes

- **Multi-server elections**
  - Add `voter_status` to peer state so voters, promotable nodes, and non-voters are distinguishable.
  - Dispatch incoming `PreVoteRpc`, `RequestVoteRpc`, and vote results through the server state machine instead of relying on unreachable clauses.
  - Persist `current_term` and `voted_for` on every term change and vote grant.
  - Reset election timeouts on accepted leader traffic and use the configured min/max election timeout.
  - Append a leader no-op entry on election so prior-term entries commit and cluster changes become permitted.
- **Durable log replication**
  - Correct follower backtracking after failed `AppendEntries` and retry immediately instead of waiting for the next tick.
  - Truncate conflicting log suffixes in both memory and the WAL.
  - Enforce voter-only commit quorum calculations.
- **Queries and auxiliary commands**
  - Implement local queries, leader queries, and linearizable consistent queries.
  - Complete the heartbeat acknowledgement path and evaluate query quorum before replying.
  - Route `aux_command`, `cast_aux_command`, and `consistent_aux` through every server state and both `handle_aux/5` and `handle_aux/6`.
- **Leadership transfer**
  - Add a `TimeoutNowRpc` and make `RaftEx.transfer_leadership/3` trigger an immediate election on the target, with the old leader stepping down on the higher term.
- **Snapshots and log lifecycle**
  - Implement checkpoint creation from machine `release_cursor` effects and on-demand compaction.
  - Persist snapshots, truncate the log and WAL, and recover the state machine from the latest snapshot.
  - Complete chunked snapshot transfer: first/middle/last chunks, per-peer transfer state, follower install, acknowledgement, and resumption of replication.
- **Cluster membership**
  - Make join default to `:voter`; add role-aware join, promote, and demote operations.
  - Require the target to be caught up before promotion.
  - Expose roles through membership queries.
- **Startup and configuration**
  - Make `RaftEx.start/1` and `RaftEx.start_in/1` operate on the `:raft_ex` OTP application and the `:raft_ex` environment instead of the inherited `:ra` names.
  - Return a usable success/error result from startup instead of silently starting a system after failed app calls.
- **Tests and docs**
  - Add multi-server integration tests using three servers on one BEAM node (elections, replication, queries, transfer, snapshots, membership).
  - Update README status and CHANGELOG.

## Capabilities

### New Capabilities

- `leader-election`: Multi-server elections with pre-vote, request-vote, durable term/vote state, election timeout handling, and leadership transfer.
- `log-replication`: AppendEntries consistency checks, conflict truncation in memory and WAL, voter-only commit advancement, leader no-op entries, and follower application.
- `raft-queries`: Local, leader, and consistent queries plus auxiliary command dispatch and replies.
- `snapshots`: Checkpoint creation, snapshot persistence, chunked transfer, follower installation, log/WAL compaction, and snapshot recovery.
- `cluster-membership`: Voter, promotable, and non-voter roles; role-aware join, leave, promote, and demote; membership information.
- `raft-startup`: Application and system startup behaviour, data-directory configuration, and startup error reporting.

### Modified Capabilities

(none)

## Impact

- Affected code: `RaftEx`, `RaftEx.App.ServerRuntime`, `RaftEx.App.Cluster`, `RaftEx.App.System`, `RaftEx.Domain.Server`, `RaftEx.Domain.Server.{Election, RpcHandler, Cluster, Config, Effects}`, `RaftEx.Domain.Types`, `RaftEx.Infra.Persistence.{Log, LogWal, LogSnapshot}`, `RaftEx.Infra.Config.Env`.
- APIs: `RaftEx.add_member/4`, `RaftEx.promote_member/3`, `RaftEx.demote_member/3`, and new `TimeoutNowRpc`; existing query and command shapes remain compatible.
- Behaviour: `RaftEx.transfer_leadership/3`, `RaftEx.consistent_query/3`, `RaftEx.local_query/3`, `RaftEx.leader_query/3`, and `RaftEx.aux_command/3` become functional instead of timing out.
- Storage: peer state gains `voter_status`; snapshots are written under each server's `snapshot/` directory; WAL truncation gains a suffix operation.
- Tests: new three-server cluster suite; existing single-server tests must keep passing.
- Documentation: README status table and CHANGELOG updated.
