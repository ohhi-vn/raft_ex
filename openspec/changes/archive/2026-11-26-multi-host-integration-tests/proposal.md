# Proposal

## Why

3-, 5-, and 7-server consensus tests pass within a single BEAM, but no test exercises a cluster spread across multiple hosts. Cross-host behaviour — node discovery, distribution, RPC routing, partitions between hosts, and recovery after a host rejoins — is unverified, so the "multi-host distributed clusters" feature remains ❌ Not Started despite the in-BEAM coverage.

## What Changes

- Add a multi-host integration test suite and harness for 3- and 5-server clusters spanning two or more hosts.
- Cover leader election, log replication, commit quorum, membership changes, snapshot transfer, and consistent queries across hosts.
- Cover host-level failure: one host rebooting/rejoining, network partition between hosts, and recovery.
- Provide a documented run procedure (cookie, epmd, node names, firewall) and an optional CI/manual profile; keep excluded from default `mix test`.
- Fix any defects surfaced (discovery, routing, timeout tuning) without changing protocol behaviour.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

(none)

No spec-level behaviour changes; this is verification of existing `leader-election`, `log-replication`, `cluster-membership`, `raft-queries`, `snapshots` requirements across hosts. `skip_specs: true` is set in `.openspec.yaml`.

## Impact

- Affected code: `test/distributed/`, `test/support/`, `lib/infra/network/` if gaps found, docs.
- APIs: no public API changes intended.
- Tests: net new multi-host tests (manual/CI profile); existing suite unchanged.
- Dependencies: none new; relies on Erlang distribution and `:peer`/remote shells.
