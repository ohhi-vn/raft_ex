# Design

## Context

See proposal.md - Why. Current code processes commands in `RaftEx.Domain.Server.handle_leader/2` (`{:command, cmd}`) and RPCs in `RaftEx.Domain.Server.handle_follower/2` plus `RaftEx.Domain.Server.RpcHandler`, with no extension points. System/server configs are plain maps (`RaftEx.App.System.default_config/0`, `RaftEx.Domain.Server.Config`).

## Goals / Non-Goals

**Goals:**
- Provide a Plug-compatible middleware abstraction and pipeline runner with halt semantics.
- Wire pipelines into command and RPC paths without altering default behavior when no plugs configured.
- Keep the core dependency-free (no hard `:plug` dependency); mirror Plug.Conn conventions.

**Non-Goals:**
- Full Plug.Conn parity or HTTP semantics.
- Rewriting Raft protocol logic.
- Requiring Plug as a dependency.

## Decisions

### D1 - Standalone Plug-compatible layer (no hard dep)

Implement `RaftEx.Domain.Plug` behaviour, `RaftEx.Domain.Conn` struct, and `RaftEx.Domain.Pipeline` runner in the domain layer (pure, no IO). Mirror Plug's `init/1`/`call/2` and `halt/1`. Do not add `:plug` as a dependency; optionally allow real Plug plugs via an adapter if `:plug` is present.

Rationale: keeps RaftEx dependency-light and testable; users familiar with Plug can reuse patterns. Alternative (add `:plug` dep) rejected: forces a web-stack dependency on a consensus library.

### D2 - Conn struct

`%RaftEx.Domain.Conn{}` with fields: `type` (`:command` | `:rpc`), `request` (map), `resp` (term), `assigns` (map), `halted` (boolean), `status` (atom/integer), `private` (map). Provide helpers `assign/3`, `halt/1`, `put_resp/2`, `get_req_header`-style accessors.

### D3 - Pipeline configuration

Add `command_plugs` and `rpc_plugs` keys to system config (list of `{module, opts}` or function plugs) and server config. Store initialized plugs (after `init/1`) in `cfg` at startup. Default `[]`.

### D4 - Integration points

- Command: in `RaftEx.Domain.Server.handle_leader({:command, cmd}, state)`, build a Conn, run `state.cfg.command_plugs`; if halted, return `{:leader, state, [reply effect with conn.resp]}` without appending. Otherwise proceed with existing append logic.
- RPC: in `RaftEx.Domain.Server.handle_follower/2` for RPC structs, run `state.cfg.rpc_plugs`; if halted, send reply from conn and skip. For candidate/pre_vote RPC handling, similar.
- Initialization of plugs happens in `RaftEx.Domain.Server.init/1` using system/server config.

### D5 - Plug declaration macro (optional)

Provide `RaftEx.Pipeline` (interface) with a `plug/2` macro for declaring pipeline modules, plus `RaftEx.Plug.Builder`-style compile-time init. Keep minimal: pipelines are ultimately lists passed via config.

## Risks / Trade-offs

- [Domain depends on Plug abstraction] -> acceptable; it's a pure utility in domain.
- [Performance overhead per command/RPC] -> Mitigation: default empty list short-circuits with no plug calls.
- [Halt semantics mismatch with Raft replies] -> Mitigation: define conn.resp contract; document expected reply shapes.
- [Config migration] -> new keys default to []; backward compatible.

## Migration Plan

1. Add Conn/Plug/Pipeline modules.
2. Add config keys + defaults.
3. Initialize plugs in Server.init.
4. Wire command pipeline in handle_leader.
5. Wire RPC pipeline in handle_follower/RpcHandler.
6. Add facade helpers and docs.
7. Tests.

## Open Questions

(none)