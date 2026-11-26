# Design

## Context

Snapshot transfer is implemented in `RaftEx.Domain.Server.RpcHandler` (`handle_install_snapshot`) and driven by `RaftEx.Domain.Server` leader-side logic, with state in `RaftEx.Infra.Persistence.LogSnapshot` / `LogSnapshotState`. Existing tests live in `test/` (unit) and `test/integration/multi_server_consensus_test.exs` (in-BEAM). There is no harness for spawning separate BEAM nodes or driving them over the network, and `test/chaos/` exists but is sparse. The `:peer` module (Erlang/OTP 25+) is available for spawning nodes without extra deps.

## Goals / Non-Goals

**Goals:**
- Reproducible distributed snapshot-transfer scenarios across 2-3 BEAM nodes.
- Cover all snapshot spec scenarios (single/multi chunk, term change, disconnect, retry, resume, checksum failure).
- Reuse existing in-BEAM test patterns; add remote-node driver only where needed.
- Keep tests in a dedicated profile that does not slow the default `mix test`.

**Non-Goals:**
- Rewriting the snapshot protocol.
- Full Jepsen-style linearizability (covered by production-hardening follow-ups).
- Multi-host CI automation (documented manual procedure is acceptable for v1).

## Decisions

- **Node spawning**: use `:peer.start/1` (OTP 25+) to start child nodes with the same code path; load the app via `-pa`/`-sname`. Fall back to `System.cmd("elixir", ...)` if `:peer` unavailable.
- **Test isolation**: place distributed tests under `test/distributed/` tagged `:distributed` and exclude by default in `test/test_helper.exs`; run via `mix test --include distributed`.
- **Driver**: a small `RaftEx.TestSupport.DistributedCluster` helper that starts N peers, starts RaftEx on each, waits for leader election, and returns node ids. Assertions are made via RPC (`:rpc.call/4`) from the test node.
- **Scenario data**: use a machine that produces a deterministic large state (e.g. repeated counter increments to index N) so snapshot size is controllable; chunk size configured small in test config to force multi-chunk paths.
- **Fault injection**: optional, gated by env var `RAFT_EX_FAULTS=1`; uses `:rpc.call` to kill peer processes or block transport. Default scenarios use graceful disconnect (peer stop) only.
- **Reproducibility**: seed printed per run; tests deterministic by construction (no timing-based assertions beyond bounded waits).

## Risks / Trade-offs

- `:peer` startup is slower; suite kept small and excluded by default.
- Flaky timing on CI; use generous timeouts and retry-once harness.
- Multi-host requires shared cookie and reachable epmd; documented, not automated in v1.
