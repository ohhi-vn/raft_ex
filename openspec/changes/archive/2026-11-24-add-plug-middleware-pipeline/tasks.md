# Tasks

## 1. Core plug abstraction

- [x] 1.1 Create `RaftEx.Domain.Conn` struct with fields and helpers (`assign/3`, `halt/1`, `put_resp/2`) and verify unit tests pass.
- [x] 1.2 Create `RaftEx.Domain.Plug` behaviour with `init/1` and `call/2` callbacks and verify compile.
- [x] 1.3 Create `RaftEx.Domain.Pipeline` runner with `run/2` and halt handling and verify unit tests pass.

## 2. Configuration

- [x] 2.1 Add `command_plugs` and `rpc_plugs` keys to system default config and server config struct and verify compile.
- [x] 2.2 Initialize plugs (calling `init/1`) in `RaftEx.Domain.Server.init/1` and store in cfg, and verify compile.
- [x] 2.3 Add facade helpers to configure pipelines and verify compile.

## 3. Command path integration

- [x] 3.1 Wire command pipeline in `handle_leader({:command, cmd}, state)` with halt handling and verify command tests pass.
- [x] 3.2 Ensure halted command returns appropriate reply to caller and verify tests pass.

## 4. RPC path integration

- [x] 4.1 Wire RPC pipeline in `handle_follower/2` for RPC structs with halt handling and verify RPC tests pass.
- [x] 4.2 Apply RPC pipeline in candidate/pre_vote RPC handling and verify tests pass.

## 5. Tests and docs

- [x] 5.1 Add tests for Conn, Plug, Pipeline, and halt semantics and verify `mix test` passes.
- [x] 5.2 Add tests for configured pipelines on command and RPC paths and verify `mix test` passes.
- [x] 5.3 Update README with plug pipeline usage examples and verify examples compile.
- [x] 5.4 Run `mix format`, `mix credo`, `mix dialyzer` and verify no new errors.