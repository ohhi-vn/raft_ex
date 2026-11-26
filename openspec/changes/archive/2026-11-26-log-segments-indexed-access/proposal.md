# Proposal

## Why

`LogSegmentWriter` persists immutable segments with fsync and supports `read_entry/2` and `read/3`, but both read paths load and parse the entire segment file (`File.read/1` + `parse_entries/1`) and filter in Elixir. As segments grow toward `segment_max_size_bytes` (default 64 MB) and `segment_max_entries` (default 4096), every single-entry or ranged read becomes O(file size) and allocates the whole file, which limits throughput for catch-up replication and snapshot-based recovery. Index-based direct access is needed to make reads proportional to the requested range.

## What Changes

- Add an in-memory (and optionally on-disk) index per segment mapping each entry index to its byte offset and length, built at write time and at recovery.
- Implement `read_entry/2` and `read/3` to seek to the indexed offset and read only the needed bytes, falling back to full parse only when the index is unavailable/corrupt.
- Persist a compact index (sidecar file or trailer) so recovery does not require re-parsing every segment; rebuild by parsing only if the index is missing/stale.
- Add index validation (checksum/count) and a repair path.
- Benchmark before/after (reuse `docs/wal-benchmark.md` harness) and document complexity and memory trade-offs.
- Keep the public API and on-disk entry format unchanged; index is an internal optimization.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

(none)

No spec-level behaviour changes: reads return the same entries; only performance characteristics change. `skip_specs: true` is set in `.openspec.yaml`.

## Impact

- Affected code: `lib/infra/persistence/log_segment_writer.ex`, `lib/infra/persistence/log.ex` (read-plan execution), tests in `test/infra/persistence/`.
- APIs: no public API changes; internal index structures added.
- Storage: optional sidecar index files alongside `.segment` files.
- Performance: reads become O(requested range + index lookup) instead of O(segment size).
- Tests: new index build/lookup/recovery/repair tests; existing segment tests must keep passing.
