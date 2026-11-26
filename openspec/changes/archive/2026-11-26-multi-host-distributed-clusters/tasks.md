# Tasks

## 1. Readiness-gated formation

- [x] 1.1 Modify `RaftEx.App.Cluster.do_form/7` so the host owning the first server id waits for all server ids to respond to `RaftEx.ping/2` before calling `trigger_election/2`, up to the configured timeout, and returns `{:error, {:servers_not_ready, ids}}` on timeout; verify with a unit/integration test that formation succeeds when all hosts are up
- [x] 1.2 Ensure hosts that do not own the first server id start their local servers and return `{:ok, local_servers}` without triggering election; verify via peer-based test that a follower-only host forms successfully
- [x] 1.3 Add `:timeout` propagation through `form_cluster/4` to the readiness wait and document the default (15 s); verify `mix test --include distributed` passes

## 2. Host-agnostic leader discovery

- [x] 2.1 Change `wait_for_leader/2` in `RaftEx.App.Cluster` to poll `RaftEx.members/2` on the first server id instead of `Leaderboard.lookup_leader/1`; verify a remote host observes the leader after formation
- [x] 2.2 Make `find_leader/1` and `find_leader_via_members/1` retry across all known members with a bounded timeout; verify `RaftEx.find_leader/1` returns `{:ok, leader}` from a non-seed host
- [x] 2.3 Keep `Leaderboard` as a local cache populated on the owning host; verify `leaderboard_overview/0` still reports the leader there

## 3. Cross-host join verification

- [x] 3.1 Add a seed reachability check at the start of `join_cluster/5` using `RaftEx.ping/2`; return `{:error, :seed_unreachable}` when the seed cannot be reached; verify with a test using an unreachable node
- [x] 3.2 Make `join_cluster/5` wait for membership propagation after adding local servers (poll `members/2` until local ids appear); verify new host appears in `members/2` on the seed host
- [x] 3.3 Ensure `join_cluster/5` does not leave half-started local servers on failure; verify cleanup on `{:error, :seed_unreachable}`

## 4. Idempotent discovery

- [x] 4.1 Guard `handle_topology_change/3` to no-op when `local_servers == []`; verify no crash when invoked on a host with no servers
- [x] 4.2 Make member addition idempotent: treat already-member errors as success and retry transient failures on the next tick; verify discovery test with a duplicate node
- [x] 4.3 Prevent discovery from attempting to start servers on remote hosts; verify logs show only local starts

## 5. Transport and diagnostics

- [x] 5.1 Add bounded retry (3 attempts) to `Transport.do_call_remote/3` and `do_send_remote/2` with fast failure; verify unreachable host returns within timeout
- [x] 5.2 Include the target node name in RPC error logs and `:noconnection` diagnostics; verify log output identifies the host
- [x] 5.3 Set default distributed operation timeouts to 15 s in `Cluster` and `ClusterSetup` while keeping `:timeout` override; verify override takes effect

## 6. Multi-host tests

- [x] 6.1 Update `test/support/distributed_cluster.ex` to support host-aware peer spawning via `:hosts` option; verify `start_peers/2` works with localhost and remote host lists
- [x] 6.2 Extend `test/integration/multi_host_scenario_test.exs` with scenarios for 5-server quorum, consistent query, snapshot transfer, partition, and reboot; verify all pass under `:peer`
- [x] 6.3 Add a join scenario test verifying a late host joins and catches up; verify membership grows to N+1
- [x] 6.4 Add discovery scenario test with env gating (`RAFT_EX_HOSTS`); verify skip when unset

## 7. Documentation and status

- [x] 7.1 Update `docs/multi-host-testing.md` with readiness, join, and recovery procedures; verify commands run as documented on a single host
- [x] 7.2 Update `README.md` status row from partial to complete for multi-host distributed clusters; verify README renders

## 8. Integration verification

- [x] 8.1 Run `mix precommit` and verify it passes
- [x] 8.2 Run `mix test --include distributed --include multi_host` and verify no new failures
