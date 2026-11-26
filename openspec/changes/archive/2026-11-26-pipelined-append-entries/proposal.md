# Proposal

## Why

RaftEx currently serializes AppendEntries per follower: the leader waits for each follower's acknowledgement before sending the next batch, even though `make_pipelined_rpc_effects` already tracks in-flight requests. Under load this caps replication throughput and leader commit latency. The README still lists pipelined AppendEntries as planned, and the archived production-hardening design (D5) specifies the intended bounded in-flight queue, out-of-order ack handling, and backpressure behaviour but it has not been implemented or specified.

## What Changes

- Specify pipelined AppendEntries behaviour in the log-replication capability: bounded in-flight request tracking per follower, out-of-order acknowledgement handling, adaptive batching, and backpressure.
- Implement leader-side in-flight tracking (`next_index`, `match_index`, pending request refs) with a configurable `pipeline_depth` (default 1 to preserve current behaviour).
- Handle AppendEntries replies that arrive out of order by advancing `match_index` only for contiguous prefixes, and retry without blocking when a follower rejects an in-flight batch.
- Add backpressure: when a follower's in-flight queue is full, skip sending, record a `replication_backpressure` metric, and retry on the next tick/reply instead of growing memory unboundedly.
- Keep all existing AppendEntries consistency, conflict truncation, commit, and backtracking requirements unchanged.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `log-replication`: add requirements for pipelined AppendEntries (bounded in-flight queue, out-of-order ack handling, adaptive batching, and backpressure).

## Impact

- Affected code: `lib/domain/server/server.ex` (RPC effect construction, reply handling), `lib/domain/server/rpc_handler.ex` (AppendEntries reply processing), `lib/domain/server/cluster.ex` (peer state), `lib/app/server_runtime.ex` (pipeline_rpcs event handling).
- Config: `max_pipeline_count` already present; may add `pipeline_depth`/adaptive batch tuning.
- Tests: `test/domain/server/*`, `test/app/server_runtime_callbacks_test.exs`, integration clusters.
- Metrics: `replication_backpressure`, `replication_lag_ms` (existing).
