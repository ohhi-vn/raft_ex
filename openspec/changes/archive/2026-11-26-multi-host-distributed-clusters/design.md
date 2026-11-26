# Design

## Context

RaftEx already ships the building blocks for distributed clusters: `RaftEx.App.Cluster` (`form_cluster/4`, `join_cluster/5`), `RaftEx.App.ClusterSetup`, `RaftEx.Infra.Network.Distribution`, `Transport`, `NodeDiscovery`, and `NodeMonitor`. The gap is that formation assumes the host owning the first server id drives election immediately, Leaderboard is node-local, and discovery/membership reconciliation are not idempotent or host-aware. See `proposal.md` for motivation and `specs/distributed-clusters/spec.md` for requirements.

## Goals / Non-Goals

**Goals:**

- Make every requirement in `distributed-clusters` pass on both `:peer` (same-host) and real multi-host runs.
- Keep single-host behaviour and existing tests unchanged.
- Fix formation ordering so election waits for all hosts to be ready.
- Make membership, discovery, and recovery host-aware and idempotent.

**Non-Goals:**

- No changes to Raft consensus semantics, WAL format, or snapshot format.
- No automatic provisioning or container orchestration.
- No shared WAL or pipelined AppendEntries (tracked separately).

## Decisions

1. **Readiness-gated formation.** The host owning the first server id SHALL wait for every server id to become reachable (via `RaftEx.ping/2`) before triggering election, up to the configured timeout. Remote hosts start their locals and return immediately. This avoids electing a leader before followers exist and keeps the existing API.

2. **Leader discovery is host-agnostic.** Formation's wait step and `find_leader/1` SHALL query a server via `members/2` (which follows leadership) rather than relying on the node-local `Leaderboard`, so remote hosts observe the leader. Leaderboard remains a fast local cache populated by the owning host.

3. **Join verifies the seed.** `join_cluster/5` SHALL ping the seed's server first; on `:noproc`/`:noconnection` it returns `{:error, :seed_unreachable}` without leaving a half-started local server. On success it adds each local server and waits for membership propagation.

4. **Discovery is idempotent and host-safe.** `handle_topology_change/3` SHALL no-op when there are no local servers, SHALL only add servers not already in membership, and SHALL never attempt to start servers on remote hosts (those hosts start their own). Errors adding a member are logged and retried on the next discovery tick.

5. **Transport reconnects on demand.** `do_send_remote/2` and `do_call_remote/3` already call `Node.connect/1` before use; keep this and add bounded retries with a fast failure when the node stays unreachable. No persistent connection pool is introduced.

6. **Partition and recovery rely on existing notifications.** `NodeMonitor`/`Transport` already emit `:nodeup`/`:nodedown`; server processes mark peers accordingly. Recovery uses existing WAL/snapshot replay, so a rebooted host rejoins with the same data directory. No new recovery code path is added; only documentation and tests.

7. **Host-aware timeouts.** Default timeouts for distributed operations increase to 15 s and remain overridable via `:timeout`. Diagnostics include the node name in log messages.

## Risks / Trade-offs

- [Readiness wait can time out if a host is slow] → Mitigation: configurable timeout, clear error identifying the unreachable server id.
- [Discovery races with manual membership changes] → Mitigation: idempotent add with already-member handling; retries on next tick.
- [Leaderboard staleness on remote hosts] → Mitigation: `find_leader/1` falls back to member queries.
- [Flaky real-host runs] → Mitigation: documented preflight checks and env-gated tests excluded by default.

## Migration Plan

No data migration. Deploy the new version; existing single-host and peer-based clusters continue to work. Operators running multi-host clusters should restart hosts with the new release and re-run formation/join as documented.

## Open Questions

None.