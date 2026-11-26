# Design

## Context

Segments are immutable files: header (24 bytes) + serialized entries (index::64, term::64, cmd_size::32, cmd_data) + (no footer currently; header has count). `read_from_segment/3` and `read_single_entry/2` call `File.read/1` on the whole file. Entry records are fixed-prefix (index, term, size) followed by variable command payload, so byte offsets are computable during write and recoverable during parse.

## Goals / Non-Goals

**Goals:**
- O(log n) or O(1) entry lookup by index within a segment.
- Range reads proportional to output size, not segment size.
- Recovery builds index without full re-parse when a sidecar index exists.
- Backward compatible with existing segment files (no index sidecar → rebuild by parsing once).

**Non-Goals:**
- Changing entry serialization format or adding footers to existing files.
- Cross-segment global index (per-segment index is sufficient; segment selection by range remains).
- Memory-mapped I/O (future optimization).

## Decisions

- **Index representation**: in-memory list of `{index, offset, length}` per segment, stored in the segment map as `:index` (list of `{first_index_offset?}`...). For lookup, binary search by index (entries written in increasing index order, so offsets are monotonic).
- **Write path**: `append_entries/2` computes offsets while serializing (accumulate byte offsets) and stores them in the segment's index. On seal, write sidecar `<segment>.idx` containing the index entries (index::64, offset::64, length::32) with a magic header + count + checksum.
- **Read path**: `read_single_entry/2` binary-searches the in-memory index, then `:file.pread/3` for `length` bytes at `offset`, then decodes one entry. `read_from_segment/3` filters index by range and preads each, or does a sequential pread over the contiguous byte range when indices are contiguous.
- **Recovery**: `load_segment_header/1` (rename/extend to `load_segment/1`) reads sidecar if present and validates count == header count and checksum; otherwise parses the segment once to rebuild the index and writes the sidecar. Cache index in segment map.
- **Repair**: if sidecar corrupt/missing, rebuild from segment (existing parse path) and rewrite sidecar; log a warning.
- **Fallback**: if index lookup fails at read time, fall back to full parse (defensive).
- **Concurrency**: reads use `:file.pread` on a freshly opened fd or a cached fd with locking; keep GenServer serialization (current design) to avoid fd sharing complexity. Consider fd pooling later.

## Risks / Trade-offs

- Sidecar files double file count and must be kept in sync on truncate/delete (delete sidecar alongside segment).
- Memory: index ~24 bytes/entry × 4096 ≈ 100 KB/segment; acceptable.
- Recovery cost: first run on legacy files parses once; subsequent runs use sidecar.
- `truncate_from`/`delete_up_to` must remove sidecars too.
