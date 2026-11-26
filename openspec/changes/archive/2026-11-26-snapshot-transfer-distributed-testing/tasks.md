# Tasks

## 1. Test infrastructure

- [x] 1.1 Add `test/support/distributed_cluster.ex` helper using `:peer` to spawn N nodes, start RaftEx, elect leader, tear down.
- [x] 1.2 Configure `test/test_helper.exs` to exclude `:distributed` tag by default and add `mix test --include distributed` usage to README/docs.
- [x] 1.3 Add a deterministic large-state machine fixture usable to force multi-chunk snapshots (small chunk size in test config).

## 2. Distributed snapshot scenarios

- [x] 2.1 Single-chunk transfer across two nodes (small snapshot fits one chunk).
- [x] 2.2 Multi-chunk transfer (first/middle/last) with small configured chunk size.
- [x] 2.3 Transfer triggered when follower `next_index` precedes leader's oldest retained entry.
- [x] 2.4 Term change on follower mid-transfer aborts transfer and steps leader down.
- [x] 2.5 Follower disconnect mid-transfer discards partial state; retry succeeds after reconnect.
- [x] 2.6 Superseded transfer replaced by newer snapshot.
- [x] 2.7 Successful installation resumes AppendEntries from snapshot+1 and counts follower toward commit.
- [x] 2.8 Checksum failure keeps previous state and no acknowledgement.

## 3. Fault injection (optional, gated)

- [x] 3.1 Add env-gated (`RAFT_EX_FAULTS=1`) network-delay/partition scenario for snapshot transfer.
- [x] 3.2 Document reproducibility (seed, timeouts) and flake-retry policy.

## 4. Defect fixes and verification

- [x] 4.1 Run distributed suite; fix any defects surfaced (race handling, retry scheduling).
- [x] 4.2 Run full `mix precommit`; ensure existing 821+ tests still pass.
- [x] 4.3 Update README feature matrix snapshot row to reflect distributed coverage.
