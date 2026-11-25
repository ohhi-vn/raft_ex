# RaftEx

> ⚠️ **EXPERIMENTAL / UNDER ACTIVE DEVELOPMENT** ⚠️
>
> This library is an experimental Elixir port of the RabbitMQ RA Raft consensus algorithm.
> It is currently in early development and **NOT READY FOR PRODUCTION USE**.

---

Raft consensus algorithm implementation in Elixir, ported from the RabbitMQ RA library ([original Erlang repo](https://github.com/rabbitmq/ra)).

## Overview

RaftEx implements the Raft consensus algorithm, providing:

- **Leader Election**: Pre-vote and candidate phases with quorum-based voting
- **Log Replication**: Write-ahead log (WAL) with batched writes and fsync support
- **Snapshotting**: Point-in-time state snapshots with chunked transfer
- **Cluster Membership**: Dynamic membership changes (join/leave) with voter, promotable and non-voter roles
- **State Machine**: Pluggable state machine interface for application logic
- **Consistent Queries**: Leader-verified reads with majority acknowledgement

## Current Status

### ✅ Implemented

| Component | Status | Description |
|-----------|--------|-------------|
| Types & RPCs | ✅ Complete | All Raft protocol message structs (AppendEntries, RequestVote, PreVote, InstallSnapshot, Heartbeat, TimeoutNow, Info) |
| WAL | ✅ Complete | Batched writes, fsync/datasync, recovery, truncation, checksums |
| Log | ✅ Complete | Write-ahead ordering, append, read, fold, snapshot, config persistence, mem table |
| Metadata Store | ✅ Complete | DETS + ETS backed term/voted_for/last_applied persistence with proper cleanup |
| Election Logic | ✅ Complete | Pre-vote, candidate, vote handling, quorum evaluation, correct vote attribution |
| RPC Handlers | ✅ Complete | AppendEntries with term/log checks, InstallSnapshot, Heartbeat handlers |
| Cluster Mgmt | ✅ Complete | Membership, voter/promotable/non-voter roles, match indexes, peer state tracking |
| Effects System | ✅ Complete | Reply routing, notifications, machine effects, consensus handling |
| Server Process | ✅ Complete | gen_statem with leader/follower/candidate/pre_vote/recover states, election tracking |
| Segment Files | ✅ Complete | Immutable segment files with fsync, write, read, seal, truncate, delete, recovery |
| State Machine | ✅ Complete | Pluggable machine behaviour with apply, tick, version, aux hooks |
| Network Layer | ✅ Complete | Distributed RPC routing, local/remote process communication, node monitoring |
| Consistent Queries | ✅ Complete | Leader-verified reads with voter quorum acknowledgement |
| Leadership Transfer | ✅ Complete | `TimeoutNowRpc` handoff to a caught-up voter with automatic step-down |
| Development Tooling | ✅ Complete | excoveralls, dialyxir, credo, `mix precommit`, quality aliases, CI-ready config |
| Tests | ✅ 821 passing | Unit + integration tests, including 3-, 5- and 7-server clusters; 98.2% line coverage |

### 🚧 In Progress

| Component | Status | Description |
|-----------|--------|-------------|
| Snapshot Transfer | 🚧 Partial | Checkpointing, recovery and chunked transfer are covered by unit and in-BEAM integration tests; broader distributed testing still needed |
| Integration Tests | 🚧 Partial | 3-, 5- and 7-server in-BEAM consensus tests pass; multi-host distributed tests still needed |
| Log Segments | 🚧 Partial | Implementation complete with fsync, needs index-based direct access optimization |
| Metrics | 🚧 Partial | Seshat counter wrapper is in place; full metrics integration is incomplete |
| Documentation | 🚧 Partial | Generated API docs and behaviour docs exist; guides and architecture deep-dive are still TODO |

### ❌ Not Started

| Component | Status | Description |
|-----------|--------|-------------|
| Performance | ❌ TODO | gen_batch_server, pipelined RPCs, batch optimizations |

## Installation

> ⚠️ This package is not yet published to Hex.pm due to its experimental status.

If available, add to your `mix.exs` (current version: `0.3.1`):

```elixir
def deps do
  [
    {:raft_ex, "~> 0.3.1"}
  ]
end
```

Or use directly from Git:

```elixir
def deps do
  [
    {:raft_ex, git: "https://github.com/manhvu/raft_ex", branch: "main"}
  ]
end
```

## Quick Start

```elixir
# Start the RaftEx system
RaftEx.start_in("/tmp/raft_data")

# Define a simple state machine
defmodule CounterMachine do
  @behaviour RaftEx.Domain.Machine

  @impl RaftEx.Domain.Machine
  def init(_conf), do: 0

  @impl RaftEx.Domain.Machine
  def apply(_meta, {:increment}, state), do: {state + 1, state + 1}
  def apply(_meta, {:get}, state), do: {state, state}
end

# Start a cluster (single node for testing)
server_id = {:server1, node()}
machine = {:machine, CounterMachine, %{}}

{:ok, _servers, _not_started} =
  RaftEx.start_cluster(:default, :my_cluster, machine, [server_id])

# Send a command
{:ok, reply, _leader} = RaftEx.process_command(server_id, {:increment})
IO.inspect(reply) # => 1
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    RaftEx.Domain.Server                     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐ │
│  │   Leader    │  │  Follower    │  │    Candidate       │ │
│  │   State     │  │  State       │  │    State           │ │
│  └──────┬──────┘  └──────┬───────┘  └────────┬───────────┘ │
│         │                │                    │             │
│  ┌──────▼────────────────▼────────────────────▼───────────┐ │
│  │              Server Process (gen_statem)               │ │
│  └──────────────────────┬───────────────────────────────┘ │
│                         │                                 │
│  ┌──────────────────────▼───────────────────────────────┐ │
│  │           RaftEx.Infra.Persistence.Log               │ │
│  │  ┌────────────┐  ┌────────────┐  ┌───────────────┐  │ │
│  │  │   WAL      │  │  Mem Table │  │   Snapshots   │  │ │
│  │  │ (GenServer)│  │   (ETS)    │  │   (Files)     │  │ │
│  │  └────────────┘  └────────────┘  └───────────────┘  │ │
│  └──────────────────────────────────────────────────────┘ │
│                         │                                 │
│  ┌──────────────────────▼───────────────────────────────┐ │
│  │              State Machine (Pluggable)                │ │
│  └──────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

- **`RaftEx`**: Main public API for cluster and server management
- **`RaftEx.App.ServerRuntime`**: `gen_statem` process managing server lifecycle
- **`RaftEx.Domain.Server`**: Core Raft protocol logic (state transitions, RPC handling)
- **`RaftEx.Infra.Persistence.Log`**: Persistent replicated log with WAL + mem table
- **`RaftEx.Infra.Persistence.LogWal`**: Write-ahead log GenServer with batched fsync
- **`RaftEx.Infra.Persistence.LogMeta`**: DETS + ETS backed metadata store
- **`RaftEx.Domain.Machine`**: Behaviour for pluggable state machines
- **`RaftEx.Domain.Server.Election`**: Leader election with pre-vote support
- **`RaftEx.Domain.Server.RpcHandler`**: AppendEntries, InstallSnapshot, Heartbeat handlers

## Configuration

`RaftEx.start/1` and `RaftEx.start_in/1` read options from the `:raft_ex`
application environment. For one release, `:ra` `:data_dir` is still accepted
as a fallback when `:raft_ex` `:data_dir` is not set.

```elixir
# Application environment (passed to RaftEx.start/1)
[
  data_dir: "/var/lib/raft_ex",
  wal_max_size_bytes: 256_000_000,
  wal_max_batch_size: 8192,
  wal_sync_method: :datasync, # :fsync | :datasync | :none
  wal_compute_checksums: true,
  segment_max_entries: 4096,
  segment_max_size_bytes: 64_000_000
]
```

```elixir
# Per-server configuration
%{
  id: {:server1, node()},
  uid: "unique_id",
  cluster_name: :my_cluster,
  initial_members: [{:server1, node()}, {:server2, node()}],
  machine: {:machine, MyMachine, %{}}
}
```

## Plug-style Middleware Pipelines

RaftEx supports composable Plug-style middleware around command and RPC processing:

```elixir
defmodule MyPlug do
  @behaviour RaftEx.Domain.Plug

  @impl RaftEx.Domain.Plug
  def init(opts), do: opts

  @impl RaftEx.Domain.Plug
  def call(conn, _opts), do: RaftEx.Domain.Conn.assign(conn, :started_at, System.monotonic_time())
end

# Plugs are part of the system configuration. Start a custom system with them:
RaftEx.App.System.stop_default()

config =
  RaftEx.App.System.default_config()
  |> Map.put(:command_plugs, [MyPlug])
  |> Map.put(:rpc_plugs, [MyPlug])

{:ok, _system} = RaftEx.App.System.start(config)
```

Plugs receive a `RaftEx.Domain.Conn` context and may call `RaftEx.Domain.Conn.halt/1` to short-circuit processing.

## API Notes

Some predicate helpers were renamed to follow Elixir conventions (`?` suffix):

| Previous | Current |
|----------|---------|
| RaftEx.Infra.Filesystem.is_dir/1 | `RaftEx.Infra.Filesystem.dir?/1` |
| RaftEx.Infra.Filesystem.is_file/1 | `RaftEx.Infra.Filesystem.file?/1` |
| RaftEx.Infra.Filesystem.is_any_file/1 | `RaftEx.Infra.Filesystem.any_file?/1` |
| RaftEx.Domain.Machine.is_versioned/1 | `RaftEx.Domain.Machine.versioned?/1` |
| RaftEx.Domain.Server.is_new?/1 | `RaftEx.Domain.Server.new?/1` |
| RaftEx.Domain.Server.is_fully_persisted?/1 | `RaftEx.Domain.Server.fully_persisted?/1` |

Snapshot persistence now returns `{:error, reason}` instead of raising for
failed chunk writes. Callers of `accept_chunk/2` and `complete_accept/2` should
handle both success and error tuples.

## Testing and Quality Gates

```bash
# Install dependencies
mix deps.get

# Run the full suite (821 tests)
mix test

# Run tests with coverage (98.2%; coveralls.json configures zero-executable-line modules)
mix coveralls

# Full pre-commit gate: compile with warnings-as-errors, format check,
# credo --strict, then the full test suite
mix precommit

# Other quality tasks
mix quality       # format, credo --strict, dialyzer
mix quality.ci    # format check, credo --strict, dialyzer

# Check formatting only
mix format --check-formatted

# Generate documentation
mix docs
```

`mix precommit` is configured as a preferred `:test` environment task in
`mix.exs`, so it can be invoked directly from a development shell.

## Specifications

Behavioural specifications live in [`openspec/specs/`](https://github.com/manhvu/raft_ex/tree/main/openspec/specs) for the
six completed capabilities:

- `cluster-membership`
- `leader-election`
- `log-replication`
- `raft-queries`
- `raft-startup`
- `snapshots`

The originating change is archived at
[`openspec/changes/archive/2026-11-25-complete-raft-consensus/`](https://github.com/manhvu/raft_ex/tree/main/openspec/changes/archive/2026-11-25-complete-raft-consensus).

## Contributing

This project is in early development. Contributions are welcome but please note:

1. This is experimental software - expect breaking changes
2. All contributions must include tests
3. Run `mix precommit` before opening a pull request
4. Follow the existing code style (`mix format`)
5. Document public APIs with `@doc` and `@moduledoc`

## License

Dual-licensed under:
- Mozilla Public License 2.0 (MPL-2.0)
- Apache License 2.0 (Apache-2.0)

See [LICENSE](https://github.com/manhvu/raft_ex/blob/main/LICENSE), [LICENSE-APACHE2](https://github.com/manhvu/raft_ex/blob/main/LICENSE-APACHE2), and [LICENSE-MPL-RabbitMQ](https://github.com/manhvu/raft_ex/blob/main/LICENSE-MPL-RabbitMQ) for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a full list of changes.

## Acknowledgments

- Original implementation: [RabbitMQ RA](https://github.com/rabbitmq/ra)
- Raft paper: [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
