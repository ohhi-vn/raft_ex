# Snapshots Specification

## Purpose

Bounds log growth by checkpointing state-machine state and restores lagging or restarted servers from a consistent point-in-time snapshot instead of replaying the full log.

## Requirements

### Requirement: Snapshot creation

When the state machine releases a cursor, an operator requests compaction, or a configured automatic trigger fires, the server SHALL persist a snapshot containing the snapshot index, term, cluster membership, machine version, and machine state.

#### Scenario: Machine releases a cursor

- **GIVEN** a server whose state machine emits a release-cursor effect at index 40
- **WHEN** the release index has been written and no snapshot transfer is in progress
- **THEN** the server writes a snapshot at index 40 and records it as current

#### Scenario: Operator triggers compaction

- **GIVEN** a running server with applied entries beyond the last snapshot
- **WHEN** an operator triggers compaction
- **THEN** the server writes a snapshot at its current applied index

#### Scenario: Automatic trigger fires

- **GIVEN** a server configured to checkpoint every 10,000 applied entries
- **WHEN** the 10,000th entry since the last snapshot is applied
- **THEN** the server writes a snapshot without an operator request

### Requirement: Log and WAL compaction

After a snapshot is persisted, the server SHALL discard log entries through the snapshot index from memory and from the write-ahead log while retaining later entries.

#### Scenario: Entries through snapshot removed

- **GIVEN** a snapshot at index 40 and log entries through index 50
- **WHEN** compaction completes
- **THEN** entries 1 through 40 are absent from memory and durable storage and entries 41 through 50 remain

#### Scenario: Recovery does not replay removed entries

- **GIVEN** a server that compacted through index 40
- **WHEN** it restarts
- **THEN** it restores the state machine from the snapshot and replays only entries after index 40

### Requirement: Snapshot recovery

On startup the server SHALL load the latest valid snapshot, initialize the state machine with the snapshot state, and set its applied and commit indexes to at least the snapshot index. A snapshot that cannot be parsed or fails validation SHALL be skipped in favor of the previous valid snapshot or the WAL, and SHALL NOT prevent startup.

#### Scenario: Valid snapshot loaded

- **GIVEN** a server directory containing a valid snapshot at index 40
- **WHEN** the server starts
- **THEN** its state machine starts from the snapshot state and its applied index is at least 40

#### Scenario: Corrupt snapshot rejected

- **GIVEN** a snapshot file whose checksum does not match
- **WHEN** the server starts
- **THEN** recovery reports the corruption and does not install partially written state

#### Scenario: Malformed snapshot does not brick startup

- **GIVEN** a snapshot file with a valid header and a malformed payload
- **WHEN** the server starts
- **THEN** recovery falls back to the previous valid snapshot or the WAL and the server starts

### Requirement: Chunked snapshot transfer

A leader SHALL send a snapshot to a follower that is missing required log entries, using first, middle, and last chunks, and SHALL track the transfer per follower. The follower SHALL accept and verify every chunk type, not only the first and last.

#### Scenario: Snapshot sent after backtracking

- **GIVEN** a follower whose next index precedes the leader's oldest retained entry
- **WHEN** the leader attempts replication
- **THEN** the leader starts a snapshot transfer instead of sending AppendEntries

#### Scenario: Multi-chunk transfer

- **GIVEN** a snapshot larger than the configured chunk size
- **WHEN** the leader transfers it
- **THEN** the follower receives a first chunk, one or more middle chunks, and a last chunk in order, and every chunk is accepted

#### Scenario: Single-chunk transfer

- **GIVEN** a snapshot that fits in one chunk
- **WHEN** the leader transfers it
- **THEN** the follower receives it as a complete snapshot

#### Scenario: Chunk checksum verified

- **GIVEN** a middle chunk whose payload was corrupted in transit
- **WHEN** the follower receives it
- **THEN** the transfer fails, the partial file is discarded, and the follower does not acknowledge the snapshot

### Requirement: Follower snapshot installation

A follower SHALL validate the snapshot checksum, install the snapshot state through the state machine's snapshot-installed callback, update its indexes, and discard the partial transfer if installation fails. Installation SHALL write to a temporary file, sync it, and rename it into place, and SHALL preserve the previous valid snapshot until the new one is durable.

#### Scenario: Successful installation

- **GIVEN** a follower receiving the final snapshot chunk with a valid checksum
- **WHEN** installation completes
- **THEN** the state machine receives the snapshot state, the follower's applied index equals the snapshot index, and it replies with the installed index and term

#### Scenario: Failed installation

- **GIVEN** a follower receiving a corrupt or incomplete snapshot
- **WHEN** installation fails
- **THEN** the follower keeps its previous state, discards the partial transfer, and does not acknowledge the snapshot

#### Scenario: Previous snapshot survives a failed install

- **GIVEN** a follower with a valid snapshot at index 40 receiving a replacement snapshot
- **WHEN** the replacement fails before its rename completes
- **THEN** the snapshot at index 40 remains readable and the follower can still restart from it

### Requirement: Replication after snapshot

After a follower installs a snapshot, the leader SHALL resume AppendEntries from the index after the snapshot and the follower SHALL participate in commit quorum again.

#### Scenario: Replication resumes

- **GIVEN** a follower that acknowledged snapshot index 40
- **WHEN** the leader sends entries starting at index 41
- **THEN** the follower accepts them and its acknowledgements count toward commit

### Requirement: Transfer failure handling

The system SHALL abandon a snapshot transfer when the receiving term changes, the follower disconnects, the transfer exceeds its timeout, or the transfer is superseded, and SHALL retry later.

#### Scenario: Stale term aborts transfer

- **GIVEN** a leader sending a snapshot to a follower
- **WHEN** the follower reports a higher term
- **THEN** the leader stops the transfer, steps down, and does not continue sending chunks

#### Scenario: Timed-out transfer retried

- **GIVEN** a transfer that has received no chunk acknowledgement within its configured timeout
- **WHEN** the timeout expires
- **THEN** the leader abandons the transfer, clears the per-peer send state, and retries on a later replication pass

### Requirement: Snapshot membership metadata

A snapshot SHALL record the cluster membership in effect at its index, and a server restoring that snapshot SHALL restore that membership rather than the configured initial members.

#### Scenario: Dynamic membership survives restart

- **GIVEN** a cluster that added member C and then snapshotted
- **WHEN** a server restarts from that snapshot
- **THEN** C is still a member with its role and quorum counting is unchanged

#### Scenario: Legacy snapshot falls back

- **GIVEN** a snapshot written before membership was recorded
- **WHEN** a server restores it
- **THEN** membership falls back to the configured initial members and the server starts

### Requirement: Snapshot write durability

Writing a snapshot SHALL use a temporary file, sync the file, atomically rename it into place, and sync the containing directory before the log and WAL are compacted through the snapshot index.

#### Scenario: Compaction follows durable snapshot

- **GIVEN** a snapshot at index 40 being written
- **WHEN** the write completes
- **THEN** the snapshot is durable before entries through index 40 are removed from the log and WAL

#### Scenario: Crash before rename

- **GIVEN** a crash after the temporary snapshot file was written but before the rename
- **WHEN** the server restarts
- **THEN** the previous snapshot remains current and the WAL is not compacted past it

### Requirement: Automatic compaction policy

The server SHALL trigger compaction when a configured entry count, WAL byte count, or time interval threshold is reached, SHALL allow an operator to override the thresholds, and SHALL NOT start a snapshot while a snapshot transfer is in progress.

#### Scenario: Entry threshold reached

- **GIVEN** a compaction threshold of 10,000 applied entries since the last snapshot
- **WHEN** the threshold is reached
- **THEN** compaction runs once and the counter resets

#### Scenario: Transfer in progress defers compaction

- **GIVEN** a snapshot transfer in progress
- **WHEN** a compaction threshold is reached
- **THEN** compaction is deferred until the transfer completes

### Requirement: Snapshot install bookkeeping

After installing a snapshot at index I and term T, the server SHALL set its last index/term, last-written index/term, snapshot state, applied index, and commit index consistently to the snapshot values, and SHALL reply with T.

#### Scenario: Install reply carries snapshot term

- **GIVEN** a follower installing a snapshot at index 40 and term 2
- **WHEN** installation completes
- **THEN** its reply reports index 40 and term 2

#### Scenario: Later snapshot send uses installed state

- **GIVEN** a follower that installed a snapshot and later became leader
- **WHEN** it needs to send a snapshot to another follower
- **THEN** it sends the installed snapshot state rather than an empty or stale state

#### Scenario: Applied index respects the snapshot

- **GIVEN** a follower whose previous last-written index was 10
- **WHEN** it installs a snapshot at index 40
- **THEN** `last_applied` is not clamped back to 10

### Requirement: Corrupt snapshot reporting

Snapshot recovery SHALL report a corrupt or unreadable snapshot and SHALL state which fallback it used, rather than silently treating the snapshot as absent.

#### Scenario: Corruption reported

- **GIVEN** a data directory whose snapshot fails validation
- **WHEN** the server starts
- **THEN** recovery reports the corruption and the fallback used

#### Scenario: Fallback is explicit

- **GIVEN** a corrupt latest snapshot and a valid earlier snapshot
- **WHEN** the server starts
- **THEN** it loads the earlier snapshot and logs that it skipped the corrupt one
