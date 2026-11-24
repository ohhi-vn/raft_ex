# Proposal

## Why

RaftEx processes commands and incoming RPCs through hard-coded paths in `RaftEx.Domain.Server` and `RaftEx.App.ServerRuntime`, with no extension point for cross-cutting concerns (logging, metrics, authorization, validation, tracing, rate limiting). Users cannot inject behavior without forking the library. Adopting a Plug-style composable middleware pipeline (like Phoenix/Plug) lets users register plugs that run around command handling and RPC handling, enabling observability and policy without modifying core Raft logic.

## What Changes

- Add a Plug-compatible middleware pipeline abstraction (`RaftEx.Plug` behaviour + `RaftEx.Pipeline` runner) and a `plug` macro for declaring pipelines.
- Add optional `{:plug, "~> 1.14"}` dependency to reuse Plug conventions (conn-like context struct) OR keep a lightweight compatible implementation if Plug is unavailable.
- Introduce configurable plug pipelines for:
  - Command path: around `process_command`/`pipeline_command` handling and state-machine apply.
  - RPC path: around incoming AppendEntries, RequestVote/PreVote, InstallSnapshot, Heartbeat handling.
- Allow per-server and per-system pipeline configuration via server/system config.
- Provide default no-op pipeline so existing behavior is unchanged when no plugs are configured.
- Add hooks for plug init (compile-time) and call (runtime), plus halt support to short-circuit processing.

## Capabilities

### New Capabilities
- `plug-pipeline`: Composable Plug-style middleware pipelines for RaftEx command and RPC processing, including plug behaviour, pipeline runner, configuration, and halt semantics.

### Modified Capabilities
(none)

## Impact

- Affected code: `RaftEx.Domain.Server`, `RaftEx.App.ServerRuntime`, `RaftEx.App.System`, `RaftEx` facade, config structs, `mix.exs` dependencies.
- APIs: additive — new `RaftEx.Plug`, `RaftEx.Pipeline`, config keys; existing command/RPC behavior unchanged by default.
- Dependencies: adds `{:plug, "~> 1.14"}` (runtime) unless a standalone compatible implementation is chosen.
- Systems: config schema extended with `:command_plugs` and `:rpc_plugs`.