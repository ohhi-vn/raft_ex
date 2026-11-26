# Proposal

## Why

RaftEx already runs clusters across multiple BEAM nodes, but the **multi-host** capability is only partial: cross-host formation, join, node discovery, RPC routing, partition handling, and recovery after a host reboot are not specified or hardened, and the README marks the feature 🚧 Partial. Completing it makes deploying a Raft cluster across physical hosts a supported, documented, and verifiable workflow rather than an ad-hoc one.

## What Changes

- Introduce a new `distributed-clusters` capability specifying externally observable behaviour for forming, joining, routing to, discovering, and recovering Raft clusters that span multiple physical hosts.
- Harden `RaftEx.App.Cluster` and the network layer (`Distribution`, `Transport`, `NodeDiscovery`, `NodeMonitor`) so formation and join work when no single host owns the first server id, with deterministic server-id mapping, seed-based election, and idempotent discovery.
- Make RPC routing resilient: reconnect to remote nodes on demand, surface `:noconnection`/`:noproc` distinctly, and propagate node up/down events to affected server processes.
- Add host-aware timeouts, diagnostics, and documented recovery procedures (host reboot/rejoin, partition heal).
- Add/extend multi-host verification (peer-based and real-host profiles) and update docs + README status from partial to complete.

## Capabilities

### New Capabilities

- `distributed-clusters`: forming and joining Raft clusters across physical hosts, deterministic cross-host server-id mapping, seed-based election, RPC routing and reconnection, node discovery, partition handling, and host reboot/rejoin recovery.

### Modified Capabilities

(none)

No changes to existing Raft protocol requirements; this capability layers cross-host deployment and recovery behaviour on top of the existing `cluster-membership`, `leader-election`, `log-replication`, `raft-queries`, and `snapshots` capabilities.

## Impact

- Affected code: `lib/app/cluster.ex`, `lib/app/cluster_setup.ex`, `lib/infra/network/distribution.ex`, `lib/infra/network/transport.ex`, `lib/infra/network/node_discovery.ex`, `lib/infra/network/node_monitor.ex`, `lib/raft_ex.ex` (delegated APIs).
- Tests: `test/support/distributed_cluster.ex`, `test/integration/multi_host_scenario_test.exs`, `test/mix/tasks/raft_ex_cluster_test.exs`.
- Docs: `docs/multi-host-testing.md`, `README.md` status row.
- APIs: no breaking changes intended; may add optional keyword options (timeouts, seeds, hosts) to `form_cluster/4` and `join_cluster/5`.
- Dependencies: none new.