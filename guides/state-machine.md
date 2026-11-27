# State Machines

Implement the `RaftEx.Domain.Machine` behaviour to define your replicated
state machine.

## Required callbacks

- `init/1` — initial state.
- `apply/3` — apply a committed command, return `{new_state, reply}` (or `{new_state, reply, effects}`).
- `snapshot/1` and `restore/2` — for snapshots.

## Auxiliary commands

Use `aux_command/2` for read-only or side-effecting operations that don't
go through consensus.

## Versioning

State machines are versioned; RaftEx tracks `machine_version` for
compatibility during upgrades. Use `noop` entries to ship a new machine
version without changing application state.
