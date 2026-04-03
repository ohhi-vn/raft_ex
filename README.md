# RaftEx

> ⚠️ **EXPERIMENTAL / UNDER ACTIVE DEVELOPMENT** ⚠️
>
> This library is an experimental Elixir port of the RabbitMQ RA Raft consensus algorithm.
> It is currently in early development and **NOT READY FOR PRODUCTION USE**.
>
> - APIs are unstable and may change without notice
> - Data formats are not finalized
> - Critical edge cases may not yet be handled
> - Distributed multi-node testing is incomplete
>
> Use at your own risk for learning, testing, and evaluation purposes only.

---

Raft consensus algorithm implementation in Elixir, ported from the RabbitMQ RA library ([original Erlang repo](https://github.com/rabbitmq/ra)).

## Overview

RaftEx implements the Raft consensus algorithm, providing:

- **Leader Election**: Pre-vote and candidate phases with quorum-based voting
- **Log Replication**: Write-ahead log (WAL) with batched writes and fsync support
- **Snapshotting**: Point-in-time state snapshots with chunked transfer
- **Cluster Membership**: Dynamic membership changes (join/leave)
- **State Machine**: Pluggable state machine interface for application logic
- **Consistent Queries**: Leader-verified reads with majority acknowledgement

## Current Status

### ✅ Implemented

| Component | Status | Description |
|-----------|--------|-------------|
| Types & RPCs | ✅ Complete | All Raft protocol message structs |
| WAL | ✅ Complete | Batched writes, fsync, recovery, truncation |
| Log | ✅ Complete | Append, read, fold, snapshot, config persistence |
| Metadata Store | ✅ Complete | DETS + ETS backed term/voted_for/last_applied |
| Election Logic | ✅ Complete | Pre-vote, candidate, vote handling, quorum eval |
| RPC Handlers | ✅ Complete | AppendEntries, InstallSnapshot, Heartbeat |
| Cluster Mgmt | ✅ Complete | Membership, voter status, match indexes |
| Effects System | ✅ Complete | Reply routing, notifications, machine effects |
| Tests | ✅ 85 passing | Unit tests for all major components |

### 🚧 In Progress

| Component | Status | Description |
|-----------|--------|-------------|
| Server Process | 🚧 Partial | Leader/follower states work, candidate/pre_vote need completion |
| Segment Files | 🚧 Stub | Long-term log storage (WAL-only currently) |
| Snapshot Transfer | 🚧 Partial | Chunked sending logic needs full implementation |
| Network Layer | 🚧 Stub | Distributed RPC between nodes not yet wired |
| Integration Tests | 🚧 Partial | Single-node tests pass, multi-node tests needed |

### ❌ Not Started

| Component | Status | Description |
|-----------|--------|-------------|
| Performance | ❌ TODO | gen_batch_server, optimizations |
| Metrics | ❌ TODO | Seshat integration incomplete |
| Documentation | ❌ TODO | API docs, guides, examples |

## Installation

> ⚠️ This package is not yet published to Hex.pm due to its experimental status.

If available, add to your `mix.exs`:

```elixir
def deps do
  [
    {:raft_ex, "~> 0.0.1"}
  ]
end
```

Or use directly from Git:

```elixir
def deps do
  [
    {:raft_ex, git: "https://github.com/your-org/raft_ex", branch: "main"}
  ]
end
```

## Quick Start

```elixir
# Start the RaftEx system
RaftEx.start_in("/tmp/raft_data")

# Define a simple state machine
defmodule CounterMachine do
  @behaviour RaftEx.Machine

  @impl RaftEx.Machine
  def init(_conf), do: 0

  @impl RaftEx.Machine
  def apply(_meta, {:increment}, state), do: {state + 1, state + 1}
  def apply(_meta, {:get}, state), do: {state, state}
end

# Start a cluster (single node for testing)
server_id = {:server1, node()}
machine = {:machine, CounterMachine, %{}}

{:ok, _} = RaftEx.start_cluster(
  :default,
  :my_cluster,
  machine,
  [server_id]
)

# Send a command
{:ok, reply, _leader} = RaftEx.process_command(server_id, {:increment})
IO.inspect(reply) # => 1
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        RaftEx.Server                        │
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
│  │                 RaftEx.Log                           │ │
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
- **`RaftEx.ServerProc`**: `gen_statem` process managing server lifecycle
- **`RaftEx.Server`**: Core Raft protocol logic (state transitions, RPC handling)
- **`RaftEx.Log`**: Persistent replicated log with WAL + mem table
- **`RaftEx.LogWal`**: Write-ahead log GenServer with batched fsync
- **`RaftEx.LogMeta`**: DETS + ETS backed metadata store
- **`RaftEx.Machine`**: Behaviour for pluggable state machines
- **`RaftEx.Server.Election`**: Leader election with pre-vote support
- **`RaftEx.Server.RpcHandler`**: AppendEntries, InstallSnapshot, Heartbeat handlers

## Configuration

```elixir
# System configuration
%{
  name: :default,
  data_dir: "/var/lib/raft_ex",
  wal_max_size_bytes: 256_000_000,
  wal_max_batch_size: 8192,
  wal_sync_method: :datasync, # :fsync | :datasync | :none
  wal_compute_checksums: true,
  segment_max_entries: 4096,
  segment_max_size_bytes: 64_000_000
}

# Server configuration
%{
  id: {:server1, node()},
  uid: "unique_id",
  cluster_name: :my_cluster,
  initial_members: [{:server1, node()}, {:server2, node()}],
  machine: {:machine, MyMachine, %{}}
}
```

## Development

```bash
# Install dependencies
mix deps.get

# Run tests
mix test

# Run tests with coverage
mix test --cover

# Check formatting
mix format --check-formatted

# Generate documentation
mix docs
```

## Contributing

This project is in early development. Contributions are welcome but please note:

1. This is experimental software - expect breaking changes
2. All contributions must include tests
3. Follow the existing code style (`mix format`)
4. Document public APIs with `@doc` and `@moduledoc`

## License

Dual-licensed under:
- Mozilla Public License 2.0 (MPL-2.0)
- Apache License 2.0 (Apache-2.0)

See [LICENSE](LICENSE), [LICENSE-APACHE2](LICENSE-APACHE2), and [LICENSE-MPL-RabbitMQ](LICENSE-MPL-RabbitMQ) for details.

## Acknowledgments

- Original implementation: [RabbitMQ RA](https://github.com/rabbitmq/ra)
- Raft paper: [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)