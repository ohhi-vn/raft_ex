# Segment read benchmark

## Before

`LogSegmentWriter.read_entry/2` and `read/3` loaded the entire segment file with
`File.read/1`, parsed every entry via `parse_entries/1`, and filtered in Elixir.
Cost per read: `O(segment_size)` time and memory, regardless of how many entries
were requested.

## After

Each segment now keeps an in-memory index of `{entry_index, byte_offset, byte_length}`.
`read_entry/2` performs a binary search (`O(log n)`) followed by a single
`:file.pread/3` of `byte_length` bytes. `read/3` filters the index by range and
preads only the requested entries. Recovery rebuilds (or loads from a sidecar
`.idx` file) the index once.

Cost per read: `O(log n + k)` where `k` is the number of entries returned, plus
one `pread` per entry. Memory is `O(n)` for the index (~24 bytes per entry).

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
