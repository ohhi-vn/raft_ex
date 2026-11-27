# Segment read benchmark

## Before

`LogSegmentWriter.read_entry/2` and `read/3` loaded the entire segment file with
`File.read/1`, parsed every entry via `parse_entries/1`, and filtered in Elixir.
Cost per read: `O(segment_size)` time and memory, regardless of how many entries
were requested. The per-batch index was also built by prepending and
concatenating lists (`new ++ old`), and the binary search had inverted
branches.

## After

Each segment keeps an in-memory index as an Erlang `:array` of
`{entry_index, byte_offset, byte_length}` tuples. `read_entry/2` performs a
binary search (`O(log n)`, direct element access) followed by a single
`:file.pread/3` of `byte_length` bytes. `read/3` opens one fd per pass,
preads only the requested entries, and closes it. Batch appends extend the
array in place instead of rebuilding lists, and the sidecar (`.idx`) is
serialized from a single iodata. Entry payloads decode with
`binary_to_term/2, [:safe]`, and unreadable or checksum-failing segments
report explicit `{:error, ...}` tuples instead of being silently skipped.

Cost per read: `O(log n + k)` where `k` is the number of entries returned, plus
one `pread` per entry (one open fd per range read). Memory is `O(n)` for the
index (~24 bytes per entry).

## Methodology

1. Write `N` entries to a single segment (N = 4096, the default
   `segment_max_entries`).
2. Time `read_entry/2` for a random index (before/after).
3. Time `read/3` for a 100-entry range (before/after).
4. Repeat with cold cache (fresh process) to measure recovery cost.

## Recovery cost

- With sidecar present: index loaded in `O(n)` from a compact file, no
  full-segment parse.
- Without sidecar (legacy files): one full parse to rebuild the index, then the
  sidecar is written for subsequent starts.

## Notes

The on-disk entry format and public API are unchanged. The sidecar (`.idx`) is
an internal optimization and is regenerated automatically if missing or corrupt.
