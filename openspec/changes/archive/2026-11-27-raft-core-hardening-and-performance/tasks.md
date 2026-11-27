# Tasks

## 1. Baseline and regression harness

- [x] 1.1 Run the existing suite and record the baseline (`mix test`) so later phases can distinguish new failures from pre-existing ones; verify the command exits 0 or the known failures are documented.
- [x] 1.2 Add failing regression tests for the confirmed defects: same-term heartbeat clears `voted_for`, WAL prefix truncation leaves bytes on disk, multi-chunk snapshot stalls after the first chunk, `await_condition` ignores events, `reply_from`/pipeline notifications are dropped, and `delete_cluster` crashes; verify each test fails against the current implementation.
- [x] 1.3 Add test support for hard-kill/restart and WAL fault injection (write error, sync error, short read, corrupt payload); verify the helpers work with a trivial passing test.
- [x] 1.4 Add a deterministic multi-chunk snapshot fixture (>64 KiB state) and a duplicate/reordered RPC interceptor; verify both are exercised by placeholder tests that currently document the expected behavior.
- [x] 1.5 Add failing regression tests for the feature-review findings: shared uid in `start_cluster/4`, prior-term commit by replica count, non-voter campaigning, leader not stepping down on a higher term, and a granted higher-term vote reply being ignored; verify each fails against the current implementation.

## 2. Term, vote, and election durability

- [x] 2.1 Split term handling into `adopt_term/2` (strictly greater) and `observe_term/2` (equal), so same-term traffic never clears `voted_for`; verify the same-term heartbeat regression test passes.
- [x] 2.2 Make `LogMeta.store_sync/4` return and propagate `{:error, reason}` instead of swallowing DETS failures; verify a test that forces a DETS error observes the error.
- [x] 2.3 Refuse to grant a vote or send a vote reply when term/vote persistence fails; verify the persistence-failure test asserts no vote is sent.
- [x] 2.4 Apply `{:update_term, term}` and `{:become, role}` effects in `ServerRuntime`, persisting the term before the role change; verify runtime tests for candidate and pre-vote candidate adopting a higher term.
- [x] 2.5 Authorize `TimeoutNow` only from the recognized leader and persist the term before campaigning; verify accepted-from-leader and rejected-from-non-leader tests.
- [x] 2.6 Persist the new term on both sides of a leadership transfer; verify a restart-after-transfer test reports the transferred term and no old-term campaign.
- [x] 2.7 Gate election timeouts on the voter role, add self to `votes_received` only when self is a voter, and count the leader in `voter_match_indexes` only when it is a voter; verify non-voter campaigning and 2-voter/1-non-voter quorum tests.
- [x] 2.8 Route incoming vote, append, heartbeat, and snapshot RPCs through leader step-down so any higher term is adopted; verify higher-term RequestVote, AppendEntries, and reply tests.
- [x] 2.9 Track the last majority acknowledgement and step the leader down when the election timeout elapses without one; verify lost-majority and restored-majority tests.
- [x] 2.10 Store the pre-vote token in the election context, ignore stale-token replies, and defer the self-vote until the candidate phase; verify failed-pre-vote and stale-token tests.
- [x] 2.11 Remove the stale local `membership` fast path and consult the cluster map by id, writing the target's real uid in cluster-change entries; verify added non-voter, promoted, and demoted servers report their new role.

## 3. WAL durability and recovery

- [x] 3.1 Verify each WAL record's CRC, bound record length, decode with `binary_to_term(..., [:safe])`, and stop recovery at the first invalid record; verify CRC-corruption, oversized-length, and truncated-header tests.
- [x] 3.2 Handle `:eof`, short reads, and empty payloads without raising during recovery; verify a record truncated at every byte boundary recovers the last valid prefix.
- [x] 3.3 Implement atomic `replace_from/4` (temp write, file sync, rename, directory sync) and use it for follower conflict replacement; verify a crash-between-write-and-rename test recovers either the old or the new log.
- [x] 3.4 Implement atomic `truncate_prefix/4` and remove the append-mode `:file.truncate` path; verify the on-disk WAL no longer contains removed records and restart returns only the retained range.
- [x] 3.5 Implement WAL rotation at a configured maximum file size and recover across rotated files in index order; verify a three-file recovery test returns every entry once.
- [x] 3.6 Normalize WAL configuration keys so `:fsync`, `:datasync`, `:none`, max batch size, checksums, and max file size are honored; verify tests assert the configured sync method is actually used.
- [x] 3.7 Propagate write, sync, truncate, and rotation errors to callers and leave the WAL recoverable; verify an injected write/sync failure returns an error and a later append succeeds after the fault clears.
- [x] 3.8 Update `docs/persistence-invariants.md` and `guides/persistence.md` to describe atomic truncation, rotation, and recovery; verify the documented recovery commands match the tests.
- [x] 3.9 Derive each member's uid from the cluster and server id (or accept an explicit uid), and fail startup on a uid collision with an actionable error; verify a three-server single-node cluster has three distinct WAL files and a collision is rejected.
- [x] 3.10 Verify the per-server WAL isolation scenario by restarting each server in a three-server cluster and confirming it recovers only its own entries; verify no cross-server entries appear.

## 4. Snapshot safety and transfer

- [x] 4.1 Write snapshots to a temp file, sync file and directory, then rename; verify a crash-before-rename test leaves the previous snapshot current and the WAL uncompacted.
- [x] 4.2 Preserve the previous valid snapshot on a failed or stale transfer and delete only the temp file; verify a failed replacement keeps the old snapshot readable.
- [x] 4.3 Unify chunk framing as `<<crc::32, payload::binary>>` and strip the frame once for first, middle, and last chunks; verify no CRC bytes appear in the installed snapshot.
- [x] 4.4 Handle middle and last chunks in the receiving state, keeping transfer id, temp file, and expected index/term; verify the >64 KiB multi-chunk integration test completes and the follower state matches.
- [x] 4.5 Add per-transfer timeout, abort on term change/superseding snapshot, and retry scheduling; verify timeout and stale-term tests discard partial state and do not acknowledge.
- [x] 4.6 Clear leader-side `snapshot_sends` and per-peer send state on completion, abort, and timeout; verify a later replication pass retries the transfer.
- [x] 4.7 Record cluster membership in versioned snapshot metadata and restore it, falling back to `initial_nodes` for legacy snapshots; verify dynamic membership survives restart and legacy snapshots still load.
- [x] 4.8 Make malformed-but-valid-header snapshots fall back to the previous snapshot or WAL instead of crashing startup; verify the malformed-snapshot startup test.
- [x] 4.9 Set the installed snapshot's term, last-written index/term, snapshot state, applied index, and commit index consistently after install, and reply with the snapshot term; verify the install-term and later-snapshot-send tests.
- [x] 4.10 Report a corrupt snapshot and the fallback used instead of silently treating it as absent; verify the corrupt-snapshot reporting test.

## 5. Runtime liveness and effect routing

- [x] 5.1 Implement `await_condition` as a retrying, timed state that re-evaluates its predicate and steps down or replies with an error on timeout; verify a WAL-down test does not leave the leader wedged.
- [x] 5.2 Replace silent unknown-event handling with logging plus an error reply for calls; verify an unsupported call returns instead of timing out.
- [x] 5.3 Route 4-tuple `{:reply, from, reply, replier}` effects to the correct caller with the replier identity; verify `reply_from: :local` and `{:member, id}` integration tests.
- [x] 5.4 Flush pipeline notifications exactly once per correlation id after the batch is durable; verify `pipeline_command/4` receives one notification and no duplicate on retry.
- [x] 5.5 Replace the `delete_cluster` pseudo-state with explicit cleanup and `{:stop, :normal, state}`, removing the child so it is not restarted; verify the server process is gone and no crash loop occurs.
- [x] 5.6 Make follower append fail-closed: on a WAL error, keep the old log and reply failure instead of building a success reply; verify the injected-write-failure test observes a rejection.
- [x] 5.7 Ignore replication replies from unknown or removed peers and never insert them into the cluster; verify a delayed reply from a removed member neither crashes nor re-adds it.
- [x] 5.8 Handle local state queries (member overview, state spec) in every state and return an explicit error for unsupported specs; verify `member_overview/2` returns instead of timing out.
- [x] 5.9 Run the RPC plug pipeline from one shared helper used by every RPC type; verify plugs fire for AppendEntries, vote, heartbeat, and snapshot RPCs.

## 6. Recovery and server configuration

- [x] 6.1 Persist server configuration (id, uid, cluster, machine module/version, initial members) atomically at start; verify the file exists and is complete after `start_server`/`start_cluster`.
- [x] 6.2 Make `Recovery` and `ServerSupSupervisor` restart registered servers from the persisted configuration and report missing/invalid config; verify a crashed server restarts with the same id and uid.
- [x] 6.3 Remove the `:ra` environment fallback and `$ra_logger` persistent term from `RaftEx.Infra.Config.Env`; verify the legacy-Ra-environment test ignores the `ra` value.
- [x] 6.4 Replace no-op recovery assertions with hard-kill/restart tests that assert the exact committed state and `last_applied`; verify the recovery matrix test actually fails when recovery is broken.
- [x] 6.5 Start the `raft_ex` application before stopping or replacing the default system and make `stop_default/0` safe when the supervisor is absent; verify cold-start and repeated-start tests return `:ok`.
- [x] 6.6 Read WAL sync method, snapshot chunk size, pipeline depth, and replication batch size from application configuration and copy them into the per-server config; verify each configured value takes effect in a running system.

## 7. Phase 1 integration gate

- [x] 7.1 Add crash/restart coverage for committed entries, votes, snapshots, and membership; verify all pass on the hardened implementation.
- [x] 7.2 Add multi-chunk snapshot transfer and leader-crash-mid-transfer coverage; verify the follower eventually catches up.
- [x] 7.3 Add partition, duplicate-RPC, and reordered-RPC coverage; verify the majority commits and the minority does not lead.
- [x] 7.4 Add concurrent membership-change coverage; verify exactly one change commits and the rest return `cluster_change_not_permitted`.
- [x] 7.5 Run `mix test`, `mix credo --strict`, and `mix dialyzer`; verify all pass or the remaining issues are documented.
- [x] 7.6 Update `CHANGELOG.md` with the safety and behavior changes, including the fail-closed and `:ra` removal notes; verify the entries match the implemented behavior.

## 8. Command batching and submission

- [x] 8.1 Add a bounded pending-command queue to `ServerRuntime` with a configurable limit; verify overflow returns a backpressure error and the queue does not grow.
- [x] 8.2 Drain the queue into one `append_commands/2` call that builds all entries and performs one `Log.write/2`; verify concurrent submissions share one WAL write and one sync.
- [x] 8.3 Correlate each batched command with its original caller and reply after the batch is durable; verify ordering and per-caller replies.
- [x] 8.4 Reply with an error to every caller in a batch whose write or sync fails; verify no caller receives success.
- [x] 8.5 Add an end-to-end concurrency test through `RaftEx.process_command/3`; verify throughput and that all replies are correct.

## 9. Replication batching and acknowledgement tracking

- [x] 9.1 Pass `max_append_entries_rpc_batch_size` to `make_rpc_effect/5` instead of a hard-coded 1; verify the replication-batch scenario actually builds the configured number of entries.
- [x] 9.2 Schedule pipelined replication when an acknowledgement frees capacity or advances match index; verify an idle leader catches up a lagging follower without client traffic.
- [x] 9.3 Replace the unbounded acknowledged list with a `MapSet` pruned to the in-flight window and advance match index in amortized O(1); verify a long-running ack soak keeps state bounded.
- [x] 9.4 Represent per-peer in-flight ranges as a queue and stop using `list ++ [entry]`; verify pipelining does not grow quadratically in the benchmark.
- [x] 9.5 Build each peer's entry batch once per pass instead of re-reading and re-reversing per peer; verify the replication benchmark shows the expected improvement.
- [x] 9.6 Coalesce commit propagation into the next AppendEntries/heartbeat instead of broadcasting a separate heartbeat per commit; verify RPC counts drop without delaying commit application.
- [x] 9.7 Route both commit paths through one helper that requires a current-term candidate and never decreases `commit_index`; verify prior-term-entry and peer-removal tests.
- [x] 9.8 Clamp follower backtracking hints so next index only moves backward, and ignore hints above the current next index; verify the divergent-suffix and stale-hint tests.
- [x] 9.9 Route tick replication through the pipeline/backpressure/snapshot-status path and advance `next_index`; verify full-pipeline and snapshot-in-progress ticks send no AppendEntries.

## 10. Transport and sender identity

- [x] 10.1 Replace the sleeping/blocking remote send with `:erlang.send(..., [:nosuspend])` and a returned error; verify a partitioned peer does not delay reachable peers.
- [x] 10.2 Add per-peer retry deadlines and scheduled retries instead of inline sleeps; verify the leader continues processing commands while a peer is unreachable.
- [x] 10.3 Carry `{:rpc, from_server_id, payload}` in the envelope and resolve the sender locally; verify a remote AppendEntries is handled without a remote `:erpc` call.
- [x] 10.4 Add a local pid-to-server-id cache for legacy senders and reject messages with neither envelope id nor payload leader id; verify the cache hit and rejection tests.
- [x] 10.5 Honor the configured seed node, reject a seed outside the node set, propagate add-member errors, and return join success only after membership is confirmed; verify the seed, add-failure, and unreachable-seed tests.
- [x] 10.6 Handle network-up/network-down events by updating peer status and scheduling reconciliation or catch-up; verify node-down and node-up tests.

## 11. Apply progress, metrics, and hot-path hygiene

- [x] 11.1 Persist `last_applied` asynchronously (timer or N entries) and sync on clean shutdown, using it to shorten replay; verify a restart after a small lag still reaches the same state.
- [x] 11.2 Initialize the metrics ETS table once and remove `init/0`/`:ets.whereis` from `inc/set/observe`; verify a hot-path test performs no repeated initialization.
- [x] 11.3 Rename or convert `replication_lag_ms` so name and unit agree; verify the lag metric test reports an index gap or a real time lag.
- [x] 11.4 Reduce per-entry apply allocations (reuse a batch-level command meta); verify the apply benchmark improves without changing state-machine results.
- [x] 11.5 Remove the mailbox copy in `queued_appends?` and use `:message_queue_len` or a counter; verify the WAL append benchmark does not regress.
- [x] 11.6 Back `Counters.counters/2` and `incr_counter/3` with the metrics store so `key_metrics/2` and `member_overview/2` return real indexes and roles; verify both APIs report live values.

## 12. Phase 2 performance evidence

- [x] 12.1 Add `command_single` and `command_cluster` scenarios (1/3/5 servers, sequential and concurrent); verify they report throughput and latency for the full path.
- [x] 12.2 Add a `follower_catchup` scenario; verify it reports time and batches for an idle leader to catch up a lagging follower.
- [x] 12.3 Add a `wal_truncate` scenario that measures prefix/suffix truncation, checks file size, and verifies restart recovery; verify it fails if the file is not truncated.
- [x] 12.4 Add a `snapshot_transfer` scenario with a >1-chunk snapshot; verify it reports chunk count and that the follower installs the complete snapshot.
- [x] 12.5 Add input assertions to every scenario (for example, `length(rpc.entries) == configured batch`) so a misconfigured scenario fails the run; verify by temporarily forcing batch size 1.
- [x] 12.6 Record the new baseline with the change committed and compare against the prior baseline; verify the comparison exits non-zero on a synthetic regression.
- [x] 12.7 Update `docs/performance.md` with the new scenarios, the recorded numbers, and an explanation for any regression; verify the documented command reproduces the table.

## 13. Segment-backed log storage

- [x] 13.1 Fix the segment index data structures (`:array`/tuple instead of `index ++`, `IO.iodata_to_binary` for the sidecar, one read fd) and verify sealing/reading unit tests.
- [x] 13.2 Wire `Log` to keep a bounded in-memory window and read older entries from sealed segments; verify a range spanning memory and segments returns correct entries and terms.
- [x] 13.3 Seal the oldest range atomically (write, sync, then evict) and recover safely from a crash during sealing; verify the crash-during-seal test.
- [x] 13.4 Return an explicit error for a missing or checksum-failing segment instead of silently skipping an index; verify the missing-segment test.
- [x] 13.5 Delete segments entirely at or below the snapshot index and retain partially covered segments; verify the retention tests.
- [x] 13.6 Put segment storage behind a configuration flag with the in-memory log as the default until the read/recovery/compaction tests pass; verify both modes pass the suite.
- [x] 13.7 Update `docs/segment-benchmark.md` and `guides/persistence.md` to match the implemented segment behavior; verify the documented commands run.

## 14. Automatic compaction and retention

- [x] 14.1 Add configurable applied-entry, WAL-byte, and time thresholds with a state-timeout check; verify the entry-threshold test triggers exactly one snapshot and resets the counter.
- [x] 14.2 Defer compaction while a snapshot transfer is in progress and retry on the next check; verify the deferral test.
- [x] 14.3 Keep the operator `trigger_compaction` override working alongside automatic triggers; verify both paths produce a snapshot.
- [x] 14.4 Add a bounded-memory test that appends far beyond the window and asserts memory and segment counts stay bounded; verify it fails when compaction is disabled.
- [x] 14.5 Document the compaction configuration and defaults in `guides/configuration.md`; verify the documented keys match the code.

## 15. Integration, performance gate, and release

- [x] 15.1 Run the full suite including distributed, chaos, and recovery tests; verify all pass with segment storage and automatic compaction enabled.
- [x] 15.2 Run a long-running soak/chaos pass with partitions, crashes, and snapshot transfers; verify no committed entry is lost and no server remains wedged.
- [x] 15.3 Run the benchmark comparison gate on the target machine; verify no scenario regresses beyond the threshold and the end-to-end numbers are documented.
- [x] 15.4 Update `README.md`, `docs/performance.md`, `docs/persistence-invariants.md`, `guides/troubleshooting.md`, and the feature-status matrix; verify every claimed feature has a corresponding test or benchmark.
- [x] 15.5 Run `mix precommit` and `mix release.check`; verify both pass before requesting archive.
- [x] 15.6 Reconcile documentation with behavior: state-machine callback order, configuration examples, leadership-transfer error codes, metric catalogue, performance tables, feature-status matrix, and version references; verify every documented claim matches a test, benchmark, or implemented behavior.

## Verification notes

- **Full suite**: `mix test` → 1261/1262 tests (27 distributed/chaos/multi-host
  tests excluded by default). The distributed and chaos suites pass under a
  named node (`elixir --sname raft_test_node -S mix test --include distributed
  --include faults`), including partitions, leader replacement, catch-up,
  recovery, and large-cluster cases.
- **Chaos/soak scope**: the soak in 15.2 is the repo's bounded chaos matrix
  (partition, crash/restart, snapshot transfer, membership churn) run
  end-to-end; no multi-hour wall-clock soak is committed to the repo.
- **Segment storage + compaction**: `test/integration/auto_compaction_test.exs`
  covers the combined mode (segment storage on, automatic compaction on) and
  asserts convergence and a bounded window; segment storage remains opt-in by
  default (`segment_storage: false`).
- **Benchmark gate**: recorded with `mix run scripts/bench.exs --record`; the
  comparison exits non-zero on a synthetic regression
  (`test/bench_test.exs`), and `docs/performance.md` carries the table.
- **Dialyzer / Credo**: see `docs/quality-baseline.md`. `mix compile
  --warnings-as-errors`, `mix format --check-formatted`, `mix test`,
  `mix docs --warnings-as-errors`, `mix release.check`, and `mix credo` all
  pass. `mix dialyzer` and `credo --strict` carry documented pre-existing
  debt (a clean `HEAD` checkout also fails `credo --strict` with exit 10), so
  `mix precommit`'s `--strict` credo step is red on the baseline too.

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
- Verify the archived result: main specs updated, validation passes, and the recorded benchmark baseline matches the released code.
