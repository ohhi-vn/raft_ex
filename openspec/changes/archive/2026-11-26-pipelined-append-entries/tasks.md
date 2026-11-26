# Tasks

## 1. Wire deferred pipeline events

- [x] 1.1 Add `leader(:info, :pipeline_rpcs, state)` clause in `lib/app/server_runtime.ex` that invokes `make_rpcs/1` and processes effects, and verify `server_runtime_callbacks_test.exs` pipeline assertion still passes.
- [x] 1.2 Guard the handler to no-op when no peers are ready, and verify with a unit test.

## 2. Per-peer in-flight tracking

- [x] 2.1 Extend peer state in `lib/domain/server/cluster.ex` with an `in_flight` list recording `{prev_index, last_index}` per request, and verify existing cluster tests pass.
- [x] 2.2 Update `make_pipelined_rpc_effects/3` and `process_peer_replication/...` in `lib/domain/server/server.ex` to append to `in_flight` and respect `max_pipeline_count`, and verify `server_coverage_test.exs` pipeline-limit test passes.
- [x] 2.3 Update `handle_append_entries_reply/2` in `lib/domain/server/rpc_handler.ex` to remove resolved requests and advance `match_index` only over contiguous prefixes, and verify new out-of-order ack tests pass.

## 3. Rejection and backpressure

- [x] 3.1 On rejection, clear the rejected in-flight entry, apply next-index hint, and schedule retry via `{:send_append_entries, peer_id}`, and verify `server_test.exs` rejection test passes.
- [x] 3.2 Increment `:replication_backpressure` when a peer is at the limit and skip sending, and verify the metric is recorded in a unit test.

## 4. Documentation and status

- [x] 4.1 Update README feature status from "Planned" to implemented for pipelined AppendEntries.
- [x] 4.2 Add changelog entry under a new version.

## 5. Integration verification

- [x] 5.1 Run a 3-server in-BEAM cluster replication test under load and confirm no regression in commit latency.
