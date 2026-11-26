# Architecture Deep-Dive

RaftEx is a pure-functional Raft implementation layered over OTP.

## Layers

- **Domain** (`lib/domain/`): pure logic — election, replication, state machine application. No processes.
- **Infra** (`lib/infra/`): persistence (WAL, segments, snapshots), networking, registry, observability.
- **App** (`lib/app/`): OTP processes — `ServerRuntime` (gen_statem), supervisors, system.
- **Facade** (`lib/raft_ex.ex`): public API.

## gen_statem lifecycle

`RaftEx.App.ServerRuntime` is a `:gen_statem` with states `:follower`, `:candidate`, `:leader`. Transitions are driven by effects returned from the domain layer.

## Effects system

Domain functions return `{new_state, effects}` where effects are tuples like
`{:send_rpc, peer, rpc}`, `{:store_meta, key, value}`, `{:become, :leader}`.
The runtime executes them. This keeps domain logic testable without OTP.

## RPC routing

RPCs are sent via `RaftEx.Infra.Network.Transport` (in-BEAM or distributed).
Replies are routed back to the originating server process.

## Data flow

1. Client calls `RaftEx.process_command/3`.
2. Runtime appends to Raft log (WAL).
3. Leader replicates via AppendEntries.
4. On quorum, commit index advances.
5. Committed entries applied to state machine.
6. Reply sent to client.