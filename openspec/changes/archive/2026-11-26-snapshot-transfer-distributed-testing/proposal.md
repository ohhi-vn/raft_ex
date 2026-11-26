# Proposal

## Why

Snapshot transfer (chunked `InstallSnapshot`) is exercised by unit and in-BEAM integration tests, but not by distributed scenarios across separate BEAM nodes or hosts. Cross-node behaviour — chunk ordering under latency, term changes mid-transfer, follower disconnect/reconnect, leader step-down, and replication resumption — is therefore unverified, leaving the transfer-failure-handling and replication-after-snapshot requirements (in `openspec/specs/snapshots`) without distributed evidence.

## What Changes

- Add a distributed snapshot-transfer test suite runnable against separate BEAM nodes on one or more hosts, covering:
  - Single-chunk and multi-chunk (first/middle/last) transfer of small and large snapshots.
  - Transfer triggered by a follower whose `next_index` precedes the leader's oldest retained entry.
  - Term change on the follower mid-transfer aborts the transfer and steps the leader down.
  - Follower disconnect mid-transfer discards partial state and retries later.
  - Superseded transfer (newer snapshot) replaces the in-flight one.
  - Successful installation resumes `AppendEntries` from snapshot index + 1 and counts the follower toward commit quorum.
  - Checksum failure keeps previous state and does not acknowledge.
- Add test support for spawning/driving remote nodes and collecting results, plus a documented matrix and seed control for reproducibility.
- Fix any defects surfaced by the new tests (expected to be minor, e.g. race handling), without changing the snapshot protocol's externally observable behaviour.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

(none)

No spec-level behaviour changes: the `snapshots` capability already specifies chunked transfer, installation, failure handling, and replication resumption. This change adds distributed verification against those requirements. `skip_specs: true` is set in `.openspec.yaml`.

## Impact

- Affected code: `test/integration/`, `test/chaos/`, `test/support/` (remote node helpers), possibly `lib/infra/network/` if a transport gap is found.
- APIs: no public API changes intended.
- Tests: net new distributed snapshot tests; existing suite must keep passing.
- Dependencies: may reuse `:peer`/`:slave` node spawning; no new required deps.
