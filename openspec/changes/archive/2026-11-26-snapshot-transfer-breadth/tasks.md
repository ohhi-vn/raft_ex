# Tasks

## 1. Follower transfer hardening

- [x] 1.1 Ensure `handle_install_snapshot_rpc/2` clears partial state on term change and replies failure; verify `rpc_handler_test.exs` stale-term case passes
- [x] 1.2 Ensure a `:first` chunk replaces an in-flight transfer (supersede) and updates `snapshot_meta`; verify unit test for superseded transfer
- [x] 1.3 Ensure middle/last chunks without in-progress state fail with `:no_snapshot_in_progress`; verify existing tests
- [x] 1.4 Ensure checksum mismatch in `LogSnapshot.recover/1` routes through `snapshot_failure/3` and keeps previous state; verify corrupt-snapshot test

## 2. Leader tracking and retry

- [x] 2.1 Mark peer status as `{:sending_snapshot, ...}` while a transfer is in flight and reset to `:normal` on completion; verify `server_coverage_test.exs` status assertions
- [x] 2.2 Clear `snapshot_sends` entry on higher-term reply, disconnect, or supersede; verify `handle_install_snapshot_reply/2` tests
- [x] 2.3 Re-initiate transfer on the next replication pass after a cleared send; verify integration scenario
- [x] 2.4 Update `match_index`/`next_index` to snapshot index + 1 on successful install and trigger `:pipeline_rpcs`; verify replication-resumes test

## 3. Unit test coverage

- [x] 3.1 Add rpc_handler tests for superseded transfer and checksum failure; verify `mix test test/domain/server/rpc_handler_test.exs`
- [x] 3.2 Add server coverage tests for peer status transitions and retry; verify `mix test test/domain/server/server_coverage_test.exs`

## 4. Distributed scenario coverage

- [x] 4.1 Implement distributed test for transfer triggered by backtracking next_index (scenario 2.3); verify `mix test --include distributed`
- [x] 4.2 Implement distributed test for term change aborts transfer and steps leader down (scenario 2.4); verify with `RAFT_EX_FAULTS=1` optional
- [x] 4.3 Implement distributed test for follower disconnect mid-transfer discards partial state and retries (scenario 2.5); verify
- [x] 4.4 Implement distributed test for superseded transfer replaced by newer snapshot (scenario 2.6); verify
- [x] 4.5 Implement distributed test for replication resumes from snapshot+1 and counts follower toward commit (scenario 2.7); verify
- [x] 4.6 Implement distributed test for checksum failure keeps previous state and no acknowledgement (scenario 2.8); verify

## 5. Fault injection and reproducibility

- [x] 5.1 Make env-gated partition/delay scenario reliable across peers; verify `RAFT_EX_FAULTS=1 mix test --include distributed --include faults`
- [x] 5.2 Document seed, timeouts, and flake-retry policy in test module docs; verify doc renders

## 6. Documentation and status

- [x] 6.1 Update README status row from partial to complete for snapshot transfer breadth; verify README
- [x] 6.2 Update `docs/` or guides if snapshot transfer behaviour changed; verify

## 7. Verification

- [x] 7.1 Run `mix test` and confirm all non-distributed tests pass
- [x] 7.2 Run `mix test --include distributed` on a single host and confirm new scenarios pass
- [x] 7.3 Run `mix precommit` and confirm clean

## Workflow follow-up

- Archive the change after review requirements are satisfied.
- Verify the archived result.