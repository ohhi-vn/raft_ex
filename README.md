# RaftEx

> ⚠️ **EXPERIMENTAL / UNDER ACTIVE DEVELOPMENT** ⚠️

Raft consensus algorithm implementation in Elixir.

## Overview

RaftEx implements Raft with leader election (pre-vote), log replication (WAL + fsync),
chunked snapshots, dynamic membership, consistent queries, leadership transfer, and
pluggable state machines. See `docs/metrics.md` and `guides/` for details.

## Status

- ✅ Core: election, replication, snapshots, membership, queries, leadership transfer, WAL
- 🚧 Partial: multi-host distributed clusters, snapshot transfer breadth
- ❌ Planned: shared WAL, pipelined AppendEntries, performance

Tests: 861 passing, ~98% coverage. See `openspec/specs/` for behavioural specs.

## Installation

```elixir
def deps do
  [{:raft_ex, "~> 0.5"}]
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

## Quality

```bash
mix deps.get
mix precommit   # compile, format, credo, tests
mix docs --warnings-as-errors
```

## License

Dual-licensed under MPL-2.0 and Apache-2.0. See LICENSE files.

## Acknowledgments

- Original: [RabbitMQ RA](https://github.com/rabbitmq/ra)
- Raft paper: https://raft.github.io/raft.pdf