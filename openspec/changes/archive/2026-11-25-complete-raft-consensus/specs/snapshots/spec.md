# Spec Delta

## Purpose

Bounds log growth by checkpointing state-machine state and restores lagging or restarted servers from a consistent point-in-time snapshot instead of replaying the full log.

## ADDED Requirements

### Requirement: Snapshot creation

When the state machine releases a cursor or an operator requests compaction, the server SHALL persist a snapshot containing the snapshot index, term, cluster membership, machine version, and machine state.

#### Scenario: Machine releases a cursor

- **GIVEN** a server whose state machine emits a release-cursor effect at index 40
- **WHEN** the release index has been written and no snapshot transfer is in progress
- **THEN** the server writes a snapshot at index 40 and records it as current

#### Scenario: Operator triggers compaction

- **GIVEN** a running server with applied entries beyond the last snapshot
- **WHEN** an operator triggers compaction
- **THEN** the server writes a snapshot at its current applied index

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

On startup the server SHALL load the latest valid snapshot, initialize the state machine with the snapshot state, and set its applied and commit indexes to at least the snapshot index.

#### Scenario: Valid snapshot loaded

- **GIVEN** a server directory containing a valid snapshot at index 40
- **WHEN** the server starts
- **THEN** its state machine starts from the snapshot state and its applied index is at least 40

#### Scenario: Corrupt snapshot rejected

- **GIVEN** a snapshot file whose checksum does not match
- **WHEN** the server starts
- **THEN** recovery reports the corruption and does not install partially written state

### Requirement: Chunked snapshot transfer

A leader SHALL send a snapshot to a follower that is missing required log entries, using first, middle, and last chunks, and SHALL track the transfer per follower.

#### Scenario: Snapshot sent after backtracking

- **GIVEN** a follower whose next index precedes the leader's oldest retained entry
- **WHEN** the leader attempts replication
- **THEN** the leader starts a snapshot transfer instead of sending AppendEntries

#### Scenario: Multi-chunk transfer

- **GIVEN** a snapshot larger than the configured chunk size
- **WHEN** the leader transfers it
- **THEN** the follower receives a first chunk, one or more middle chunks, and a last chunk in order

#### Scenario: Single-chunk transfer

- **GIVEN** a snapshot that fits in one chunk
- **WHEN** the leader transfers it
- **THEN** the follower receives it as a complete snapshot

### Requirement: Follower snapshot installation

A follower SHALL validate the snapshot checksum, install the snapshot state through the state machine's snapshot-installed callback, update its indexes, and discard the partial transfer if installation fails.

#### Scenario: Successful installation

- **GIVEN** a follower receiving the final snapshot chunk with a valid checksum
- **WHEN** installation completes
- **THEN** the state machine receives the snapshot state, the follower's applied index equals the snapshot index, and it replies with the installed index and term

#### Scenario: Failed installation

- **GIVEN** a follower receiving a corrupt or incomplete snapshot
- **WHEN** installation fails
- **THEN** the follower keeps its previous state, discards the partial transfer, and does not acknowledge the snapshot

### Requirement: Replication after snapshot

After a follower installs a snapshot, the leader SHALL resume AppendEntries from the index after the snapshot and the follower SHALL participate in commit quorum again.

#### Scenario: Replication resumes

- **GIVEN** a follower that acknowledged snapshot index 40
- **WHEN** the leader sends entries starting at index 41
- **THEN** the follower accepts them and its acknowledgements count toward commit

### Requirement: Transfer failure handling

The system SHALL abandon a snapshot transfer when the receiving term changes, the follower disconnects, or the transfer is superseded, and SHALL retry later.

#### Scenario: Stale term aborts transfer

- **GIVEN** a leader sending a snapshot to a follower
- **WHEN** the follower reports a higher term
- **THEN** the leader stops the transfer, steps down, and does not continue sending chunks
