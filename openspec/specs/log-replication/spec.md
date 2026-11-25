# Log Replication Specification

## Purpose

Replicates log entries from the leader to followers with Raft's consistency guarantees, commits only voter-acknowledged entries, and applies committed entries in order.

## Requirements

### Requirement: AppendEntries consistency check

A follower SHALL accept an AppendEntries request only when it has an entry matching the leader's previous index and term, and SHALL reject it otherwise without modifying its log.

#### Scenario: Matching prefix accepted

- **GIVEN** a follower whose log matches the leader's previous index and term
- **WHEN** the leader sends new entries
- **THEN** the follower appends them and replies success with its new last index and term

#### Scenario: Missing prefix rejected

- **GIVEN** a follower that does not have the leader's previous index
- **WHEN** the leader sends entries
- **THEN** the follower leaves its log unchanged and replies failure with a lower next index

#### Scenario: Conflicting term rejected

- **GIVEN** a follower with a different term at the leader's previous index
- **WHEN** the leader sends entries
- **THEN** the follower replies failure so the leader can backtrack

### Requirement: Conflict truncation

Before appending a replacement entry, a follower SHALL remove every conflicting entry from that index onward in both its in-memory log and its durable write-ahead log.

#### Scenario: Conflicting suffix removed

- **GIVEN** a follower whose entries at index 5 and 6 conflict with the leader's entry at index 5
- **WHEN** the follower accepts the leader's new entry at index 5
- **THEN** entries 5 and 6 are removed from memory and durable storage before the new entry is written

#### Scenario: Restart does not resurrect conflicts

- **GIVEN** a follower that truncated a conflicting suffix
- **WHEN** the follower restarts
- **THEN** the truncated entries are not recovered from the WAL

### Requirement: Voter-only commit advancement

The leader SHALL advance its commit index only for an entry from its current term that is stored on a majority of voters, and SHALL NOT count non-voters or promotable members.

#### Scenario: Majority of voters commits

- **GIVEN** a leader and two voters besides itself
- **WHEN** one voter acknowledges the current-term entry
- **THEN** the leader advances the commit index to that entry

#### Scenario: Non-voter acknowledgement ignored

- **GIVEN** a leader with two voters and one non-voter
- **WHEN** only the non-voter acknowledges an entry
- **THEN** the leader does not advance the commit index

#### Scenario: Prior-term entry not committed by count alone

- **GIVEN** an entry from an earlier term replicated to a majority
- **WHEN** no current-term entry has committed
- **THEN** the leader does not commit the earlier entry

### Requirement: Follower commit application

A follower SHALL apply committed entries up to the lower of the leader's commit index and its own last log index, and SHALL persist its applied index.

#### Scenario: Follower applies leader commit

- **GIVEN** a follower with entries through index 10 and a leader commit index of 8
- **WHEN** it processes the AppendEntries or Heartbeat
- **THEN** it applies entries through index 8 and records 8 as applied

#### Scenario: Commit beyond local log clamped

- **GIVEN** a follower whose last log index is 4 and a leader commit index of 9
- **WHEN** it processes the leader's commit index
- **THEN** it applies only through index 4

### Requirement: Prompt log backtracking

When an AppendEntries request is rejected, the leader SHALL reduce the follower's next index using the follower's hint or by one, and SHALL retry replication without waiting for the next periodic tick.

#### Scenario: Retry after rejection

- **GIVEN** a leader whose follower rejected entries at next index 10
- **WHEN** the rejection carries next index 6
- **THEN** the leader immediately sends entries starting at index 6

### Requirement: Heartbeat commit propagation

The leader SHALL include its commit index in periodic heartbeats and a follower SHALL apply newly committed entries from that index.

#### Scenario: Idle follower learns commit

- **GIVEN** a follower that has replicated entries but has not applied them
- **WHEN** it receives a heartbeat with a higher commit index
- **THEN** it applies the newly committed entries without waiting for another AppendEntries payload
