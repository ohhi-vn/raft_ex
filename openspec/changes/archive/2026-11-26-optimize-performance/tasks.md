# Tasks

## 1. Benchmark baseline (measure before optimizing)

- [x] 1.1 Add `scripts/bench.exs` and `bench/` scenario modules for WAL append, WAL recovery, log fold, apply, replication batch construction, and query; verify `mix run scripts/bench.exs --scenario wal_append` prints one result row.
- [x] 1.2 Implement warm-up, fixed iteration counts, isolated temp directories, JSON output, `--record`, `--compare`, and `--threshold` with a non-zero exit on regression; verify against a hand-written baseline that a simulated regression exits non-zero.
- [x] 1.3 Add `test/bench_test.exs` running each scenario with a tiny iteration count and asserting the report shape and comparison exit codes; verify with `mix test test/bench_test.exs`.
- [x] 1.4 Record the pre-change baseline with `mix run scripts/bench.exs --record` and document the command, machine, OTP/Elixir versions, and table in `docs/performance.md`; verify the documented command runs as written.

## 2. WAL buffering, flush, and recovery

- [x] 2.1 Replace `buffer ++ entries` with a reversed buffer and explicit `buffer_size`; add tests asserting entry order, O(1) batch-limit checks, and that the buffer never exceeds `wal_max_batch_size` (`mix test test/infra/persistence/log_wal_test.exs`).
- [x] 2.2 Serialize each flush batch once and reuse the binary for header, CRC, write, and byte metrics; verify all WAL tests pass and the `wal_append` benchmark improves over the baseline.
- [x] 2.3 Open WAL files without `:delayed_write` and implement deferred group commit with a guarded zero-delay flush; add a test that N concurrent appends produce fewer `:wal_fsyncs` than N and that every caller receives its own range (`mix test test/infra/persistence/log_wal_test.exs`).
- [x] 2.4 Reply `{:error, reason}` to every queued caller when a write or sync fails, leaving the buffer for retry; verify with a test that forces a closed/invalid fd and asserts no caller sees success.
- [x] 2.5 Make recovery accumulate decoded records in reverse and reverse once; add a 10,000-record recovery test plus a torn-trailing-record test asserting complete records only and ascending order (`mix test test/infra/persistence/log_wal_test.exs`).
- [x] 2.6 Reuse linear recovery in `truncate_from/3` and add prefix/suffix truncation tests that restart the WAL and assert the recovered range (`mix test test/infra/persistence/log_wal_test.exs`).
- [x] 2.7 Update `guides/persistence.md` to describe group commit and acknowledgement semantics; verify the documented behavior matches the tests.

## 3. Log hot path

- [x] 3.1 Replace the filter-and-sort in `Log.fold/6` with index-range lookups and add the sparse-log fallback; add tests for a dense range, a range with gaps, and a range larger than the map (`mix test test/infra/persistence/log_test.exs`).
- [x] 3.2 Batch the mem-table insert in `Log.write/2` into one `:ets.insert/2` and merge new entries into the map in one pass; verify existing log and server tests pass and the `log_fold`/`apply_committed` benchmarks improve.
- [x] 3.3 Derive the recovered last term from the WAL last index instead of `Enum.max_by/2`; verify startup recovery tests and the `wal_recovery` benchmark.
- [x] 3.4 Replace whole-map rejection in `Log.truncate/2` with `Map.drop/2` and last-index probing, and clear truncated indexes from the mem table; add a conflict-truncation test asserting the new range, last term, and mem-table contents.

## 4. Replication and commit application

- [x] 4.1 Rework `RpcHandler.append_entries/3` to find the first missing or mismatched index, truncate once, and write the remaining entries with one `Log.write/2`; add a test where the first entry conflicts and assert the resulting log, one `:wal_flushes` increment, and no duplicate entries (`mix test test/domain/server/rpc_handler_test.exs`).
- [x] 4.2 Keep leader replication reads to the requested batch via the new `Log.fold/6`; add a test asserting the AppendEntries payload contains exactly the requested entries and verify the `replication_batch` benchmark.
- [x] 4.3 Remove the per-peer context map allocation in `make_pipelined_rpc_effects/3` by passing arguments directly; verify existing multi-server and replication tests pass.
- [x] 4.4 Update `guides/architecture.md` and `guides/persistence.md` where they describe per-entry appends or truncation; verify the descriptions match the new code paths.

## 5. Metrics

- [x] 5.1 Switch counters to `:ets.update_counter/4` with a default tuple and make `Metrics.init/0` an error-path retry; add a test with 100 processes × 100 increments asserting the final value is 10,000 and no process crashes (`mix test test/infra/observability_test.exs` or the existing metrics test file).
- [x] 5.2 Update histogram count and sum atomically and keep min/max best-effort; add a concurrent-observation test asserting count and sum are exact.
- [x] 5.3 Document counter atomicity and best-effort histogram extremes in `docs/metrics.md`; verify the document matches the implemented behavior.

## 6. Metadata and startup

- [x] 6.1 Group-commit `LogMeta.store_sync/4` callers around one DETS sync while replying only after durability; add a concurrent-store test and a restart test asserting persisted values (`mix test test/infra/persistence/log_meta_test.exs`).
- [x] 6.2 Load segment headers with `:file.pread/3` and use the sidecar index without reading the full segment; add recovery tests for sidecar-present and sidecar-missing segments (`mix test test/infra/persistence/log_segment_writer_test.exs`).
- [x] 6.3 Build the segment index with a reversed accumulator instead of `idx ++ new_index_entries`; verify segment write/read tests and the `wal_recovery` benchmark.

## 7. Integration, benchmarks, and docs

- [x] 7.1 Run `mix precommit` (compile with warnings as errors, format check, Credo strict, full test suite) and fix any regressions.
- [x] 7.2 Re-run the benchmark suite, record the post-change baseline, and add a before/after table with the command and environment to `docs/performance.md`; verify the numbers match the recorded baseline files.
- [x] 7.3 Update `README.md` status and `CHANGELOG.md` with the performance changes; verify `mix docs --warnings-as-errors` passes.
- [x] 7.4 Run `openspec validate optimize-performance --strict` and confirm the change and all spec deltas validate.

## Workflow follow-up

- Archive the change after review and after the implementation is merged.
- Verify the archived specs appear under `openspec/specs/` with the intended requirements.
