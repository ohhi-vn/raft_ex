# Design

## Context

Multi-host testing depends on Erlang distribution: named nodes, shared cookie, epmd, and reachable ports. RaftEx already has `RaftEx.Infra.Network.Distribution`, `NodeDiscovery`, `NodeMonitor`, and `Transport` abstractions. Existing in-BEAM tests use anonymous nodes; multi-host requires `-sname`/`-name` nodes with a common cookie. The `:peer` module cannot spawn remote processes across hosts, so remote nodes must be started via SSH or a documented manual procedure.

## Goals / Non-Goals

**Goals:**
- A reproducible procedure to run a 3/5-server cluster across 2+ hosts.
- Automated where possible (SSH-driven) and manual fallback documented.
- Assertions via RPC from a coordinator node.
- Clear failure diagnostics (which host/node failed).

**Non-Goals:**
- Automated cloud provisioning.
- Performance benchmarking across hosts (separate concern).
- Changing Raft protocol.

## Decisions

- **Node startup**: provide `scripts/start_raft_node.sh` (or `mix` task) that boots a named node with cookie and code path; coordinator connects via `Node.connect/1`.
- **Automation**: optional SSH-based launcher in `test/support/multi_host.ex` guarded by env `RAFT_EX_HOSTS="host1,host2"`; skips gracefully when unset.
- **Discovery**: rely on explicit node list passed to test; no multicast discovery required for v1.
- **Partition simulation**: use `:erlang.disconnect_node/1` or firewall rules; document both. In-BEAM tests use `Node.disconnect/1`.
- **Timeouts**: host-aware (longer) defaults; configurable via env.
- **Profile**: tag `:multi_host`, excluded by default; run via `mix test --include multi_host`.

## Risks / Trade-offs

- Environment-dependent; cannot run in standard CI without infra. Documented as manual/optional.
- Flakiness from DNS/epmd; provide preflight checks.
- Security: shared cookie in docs; warn against production use.
