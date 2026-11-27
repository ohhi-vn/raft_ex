# RaftEx

Raft consensus algorithm implementation in Elixir.

## Overview

RaftEx implements Raft with leader election (pre-vote), log replication (WAL + fsync),
chunked snapshots, dynamic membership, consistent queries, leadership transfer, and
pluggable state machines. See `docs/metrics.md` and `guides/` for details.

## Features

- Leader election with pre-vote
- Log replication with per-server WAL and fsync
- Chunked snapshot transfer (single and multi-chunk)
- Dynamic membership changes (add/remove/promote/demote)
- Consistent and local queries
- Leadership transfer
- Pluggable versioned state machines
- Multi-host distributed clusters
- Pipelined AppendEntries with backpressure
- Observability and metrics
- Performance benchmark suite

## Feature status

Every row is backed by an automated test or benchmark; see
`test/`, `bench/`, and `openspec/changes/raft-core-hardening-and-performance/`.

| Capability | Status | Evidence |
|------------|--------|----------|
| Term/vote persistence, fail-closed voting | done | `test/domain/server/election_persistence_test.exs` |
| Unique per-server WAL identity | done | `test/integration/server_uid_isolation_test.exs` |
| Voter-only elections, leader step-down, check-quorum | done | `test/domain/server/check_quorum_test.exs`, `test/domain/server/leader_stepdown_test.exs` |
| CRC-verified WAL recovery, atomic truncation, rotation | done | `test/infra/persistence/log_wal_*_test.exs` |
| Atomic snapshots, multi-chunk transfer with timeouts | done | `test/domain/server/snapshot_hardening_test.exs`, `test/integration/distributed_snapshot_transfer_test.exs` |
| Command batching with backpressure | done | `test/domain/server/command_batch_test.exs`, `test/app/server_runtime_batch_test.exs`, `bench` `command_*` |
| Bounded replication (batch sizing, ack tracking, no transport sleeps) | done | `test/domain/server/replication_batching_test.exs` |
| Sender-identity envelopes, non-blocking transport, seed validation, network events | done | `test/app/transport_identity_test.exs` |
| Metrics store, per-server counters, lag/apply gauges | done | `test/infra/observability/coverage_test.exs`, `test/integration/apply_metrics_test.exs` |
| End-to-end benchmarks with input assertions | done | `bench/scenarios.ex`, `test/bench_test.exs`, `docs/performance.md` |
| Segment-backed storage (opt-in) | done | `test/infra/persistence/log_segment_storage_test.exs` |
| Automatic compaction with deferral + operator override | done | `test/integration/auto_compaction_test.exs` |

## Installation

```elixir
def deps do
  [{:raft_ex, "~> 0.6"}]
end
```

## Quick Start

```elixir
RaftEx.start_in("/tmp/raft_data")

server_id = {:server1, node()}
machine = {:machine, MyMachine, %{}}
{:ok, _servers, []} = RaftEx.start_cluster(:default, :my_cluster, machine, [server_id])

{:ok, reply, _leader} = RaftEx.process_command(server_id, {:increment})
```

## Documentation

- Guides: `guides/` (getting-started, architecture, persistence, membership, ...)
- API docs: `mix docs`
- Metrics: `docs/metrics.md`
- Changelog: `CHANGELOG.md`

## License

Licensed under Apache-2.0.

## Acknowledgments

- Raft paper: https://raft.github.io/raft.pdf
