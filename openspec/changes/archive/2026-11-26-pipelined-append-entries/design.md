# Design

## Context

RaftEx already implements a pipelined RPC effect builder (`make_pipelined_rpc_effects/3` in `lib/domain/server/server.ex`) with in-flight tracking via `next_index`, `match_index`, `commit_index_sent`, and a `backpressure_count`. The `:pipeline_rpcs` next-event is emitted after WAL writes but is not handled in `lib/app/server_runtime.ex`, so deferred pipeline work never runs. AppendEntries replies are processed in `RaftEx.Domain.Server.RpcHandler.handle_append_entries_reply/2`, which currently assumes a single in-flight request per peer and does not reconcile out-of-order acknowledgements.

## Goals / Non-Goals

**Goals:**
- Make the existing pipeline machinery actually drive replication, including the deferred `:pipeline_rpcs` event.
- Track in-flight requests per peer so out-of-order replies can be reconciled and match index advances only over contiguous prefixes.
- Preserve default behaviour (depth 1) so existing tests and recovery semantics are unchanged.
- Keep backpressure bounded and observable.

**Non-Goals:**
- Adaptive batching heuristics beyond the existing `effective_batch` calculation; tune constants only.
- Changing AppendEntries protocol or follower-side logic.
- Multi-host transport changes.

## Decisions

### D1: Handle `:pipeline_rpcs` in ServerRuntime
**Choice:** Add a `leader(:info, :pipeline_rpcs, state)` clause that calls `make_rpcs/1` (the existing `Server.make_rpcs/1` wrapper) and processes resulting effects.
**Rationale:** The event is already emitted; wiring it completes the pipeline loop without new scheduling.
**Alternatives:** Polling on tick only — rejected because it adds latency and ignores the emitted event.

### D2: Per-peer in-flight request set
**Choice:** Store a list/map of pending request refs on the peer struct (`in_flight`), each recording `{prev_index, last_index}`. Advance `match_index` to the highest contiguous acknowledged index.
**Rationale:** Enables out-of-order ack handling required by the spec.
**Alternatives:** Single `next_index`/`match_index` (current) — insufficient for out-of-order reconciliation.

### D3: Backpressure via existing counter
**Choice:** Reuse `backpressure_count` (decrement on each pipeline pass) and increment the `:replication_backpressure` metric when a peer is at the limit.
**Rationale:** Already implemented and tested in `server_coverage_test.exs`.

### D4: Default depth 1
**Choice:** Keep `max_pipeline_count` default but treat effective depth as 1 unless configured higher, preserving current behaviour for existing tests.
**Rationale:** Risk mitigation from production-hardening D5.

## Risks / Trade-offs

- [Out-of-order ack bugs] → Extensive unit tests in `server_test.exs` and integration clusters; keep depth default 1.
- [Deferred event storms] → Guard `:pipeline_rpcs` handler to no-op when no pending work.
- [Memory growth] → Bounded by `max_pipeline_count` and `backpressure_count`.

## Migration Plan

1. Wire `:pipeline_rpcs` handler.
2. Add in-flight tracking to peer state and RpcHandler reply path.
3. Update tests for out-of-order acks.
4. Update README feature status from planned to implemented.

## Open Questions

- Exact default for adaptive batch growth constant (currently `effective_batch = max(1, min(batch_size, max_pipeline - in_flight))`).
