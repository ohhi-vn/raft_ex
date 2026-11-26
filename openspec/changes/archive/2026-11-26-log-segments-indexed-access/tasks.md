# Tasks

## 1. Index data structure

- [x] 1.1 Add `:index` field to segment map (`[{index, offset, length}]`).
- [x] 1.2 Implement binary search helper for index lookup by entry index.
- [x] 1.3 Update `append_entries/2` to record byte offsets/lengths while serializing.

## 2. Sidecar persistence

- [x] 2.1 Define sidecar format (magic, version, count, entries, checksum) and write on seal.
- [x] 2.2 Load sidecar in recovery; validate against header; rebuild + rewrite if invalid/missing.
- [x] 2.3 Delete sidecar alongside segment in `truncate_from` and `delete_up_to`.

## 3. Read paths

- [x] 3.1 Rewrite `read_single_entry/2` to use index + `:file.pread`.
- [x] 3.2 Rewrite `read_from_segment/3` to use index for ranged reads (sequential pread over contiguous range).
- [x] 3.3 Add fallback to full parse on index miss/corruption.

## 4. Validation and tests

- [x] 4.1 Add index build/lookup unit tests.
- [x] 4.2 Add recovery tests (sidecar present, missing, corrupt).
- [x] 4.3 Add repair test (corrupt sidecar rebuilt from segment).
- [x] 4.4 Add range-read correctness tests (compare with full parse).
- [x] 4.5 Benchmark before/after and record in `docs/wal-benchmark.md` or new `docs/segment-benchmark.md`.
- [x] 4.6 Run `mix precommit`.
