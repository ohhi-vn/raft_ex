# Proposal

## Why

Snapshot transfer (chunked `InstallSnapshot`) is implemented and covered by unit tests, but its distributed behaviour is only partially verified: the distributed suite exercises two of the eight planned scenarios, and the remaining failure paths (term change mid-transfer, follower disconnect, superseded transfer, checksum failure) are either untested or not hardened for cross-node races. This leaves the `snapshots` capability's transfer-failure-handling and replication-after-snapshot requirements without distributed evidence and keeps "snapshot transfer breadth" marked partial in the project status.

## What Changes

- Harden follower-side transfer state so a mid-transfer term change, a superseding (newer) snapshot, or a checksum failure discards partial state, cleans up the partial file, and does not acknowledge.
- Harden leader-side transfer state so a follower disconnect, a higher-term reply, or a superseded snapshot abandons the in-flight transfer, cleans up `snapshot_sends`, and schedules a retry on the next replication pass.
- Make `AppendEntries` resumption after a successful install deterministic (next_index = snapshot index + 1) and re-count the follower toward commit quorum.
- Add distributed tests for all eight scenarios from the prior change (single/multi-chunk, backtracking trigger, term-change abort, disconnect retry, superseded replacement, replication resumption, checksum failure), plus unit tests for the new race paths.
- Document reproducibility (seed, timeouts, flake-retry policy) and update the README status row from partial to complete.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

(none)

No spec-level behaviour changes: the `snapshots` capability already specifies chunked transfer, installation, failure handling, and replication resumption. This change implements and verifies breadth against those requirements. `skip_specs: true` is set in `.openspec.yaml`.

## Impact

- Affected code: `lib/domain/server/rpc_handler.ex`, `lib/domain/server/server.ex`, `lib/app/server_runtime.ex`, `lib/infra/persistence/log_snapshot.ex`.
- Tests: `test/integration/distributed_snapshot_transfer_test.exs`, `test/integration/distributed_fault_snapshot_test.exs`, `test/domain/server/rpc_handler_test.exs`, `test/domain/server/server_coverage_test.exs`.
- APIs: no public API changes intended.
- Dependencies: no new dependencies.