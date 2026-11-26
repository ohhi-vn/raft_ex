# RaftEx

> ⚠️ **EXPERIMENTAL / UNDER ACTIVE DEVELOPMENT** ⚠️

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
