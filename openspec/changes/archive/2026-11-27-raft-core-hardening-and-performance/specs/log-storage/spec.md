# Spec Delta

## Purpose

Defines how RaftEx keeps the logical log bounded in memory, persists cold entries to sealed segments, serves reads across the in-memory and on-disk tiers, and retires segments that compaction has made obsolete.

## ADDED Requirements

### Requirement: Bounded in-memory log

The server SHALL retain in memory only a configured window of the most recent log entries and SHALL serve older entries from durable segments, so memory use does not grow with the total log length.

#### Scenario: Memory stays bounded

- **GIVEN** a server with an in-memory window of 1,000 entries
- **WHEN** 100,000 entries have been appended
- **THEN** at most 1,000 entries are held in memory and the rest are served from segments

#### Scenario: Old entry remains readable

- **GIVEN** an entry that has been moved out of the in-memory window
- **WHEN** the leader or a follower requests that index and term
- **THEN** the entry is read from its segment and returned with the correct index and term

### Requirement: Atomic segment sealing

When the in-memory window exceeds its threshold, the server SHALL write the oldest entries to a new segment, sync it, and only then drop them from memory, so a crash cannot leave an entry in neither tier.

#### Scenario: Seal then evict

- **GIVEN** an in-memory window at its threshold
- **WHEN** another entry is appended
- **THEN** the oldest entries are sealed and synced to a segment before they are removed from memory

#### Scenario: Crash during sealing

- **GIVEN** a crash while a segment is being written
- **WHEN** the server restarts
- **THEN** recovery ignores the incomplete segment and retains the entries still in memory or the previous segment

### Requirement: Segment reads and index lookup

Reading a range that spans the in-memory and segment tiers SHALL return entries in ascending index order, SHALL read only the segments covering the requested range, and SHALL locate entries without scanning every segment.

#### Scenario: Range spans tiers

- **GIVEN** entries 900 through 1,100 where 900 through 999 are sealed and 1,000 through 1,100 are in memory
- **WHEN** entries 950 through 1,050 are read
- **THEN** exactly those entries are returned in order and no unrelated segment is read

#### Scenario: Missing segment reported

- **GIVEN** a requested index whose segment file is missing or fails its checksum
- **WHEN** the read runs
- **THEN** the read returns an explicit error rather than silently skipping the index

### Requirement: Segment retention after compaction

After a snapshot at index N is durable, the server SHALL delete segments whose entries are entirely at or below N and SHALL retain segments containing entries above N.

#### Scenario: Obsolete segments removed

- **GIVEN** segments covering entries 1 through 40 and a durable snapshot at index 40
- **WHEN** compaction completes
- **THEN** those segments are deleted and segments covering entries above 40 remain

#### Scenario: Partially covered segment retained

- **GIVEN** a segment covering entries 35 through 60 and a snapshot at index 40
- **WHEN** compaction completes
- **THEN** the segment is retained and entries above 40 remain readable
