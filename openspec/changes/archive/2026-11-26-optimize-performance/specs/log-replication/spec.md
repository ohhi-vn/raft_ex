# Spec Delta

## ADDED Requirements

### Requirement: Batch-proportional replication work

Replication SHALL read only the entries in the requested AppendEntries range, SHALL write the acceptable entries of one AppendEntries request in a single WAL write, and SHALL apply only the newly committed index range.

#### Scenario: Leader reads only the batch

- **GIVEN** a leader with 10,000 log entries and a follower that needs entries 9,001 through 9,010
- **WHEN** the leader builds the AppendEntries request
- **THEN** it reads ten entries and sends those ten entries

#### Scenario: Follower writes one batch

- **GIVEN** a follower receiving ten acceptable entries in one AppendEntries request
- **WHEN** it appends them
- **THEN** it performs one WAL write for the ten entries

#### Scenario: Apply only new commits

- **GIVEN** a server with `last_applied` 500 and a new commit index of 550
- **WHEN** it applies committed entries
- **THEN** it applies indexes 501 through 550 only

### Requirement: Monotonic hot-path cost

The cost of appending one entry, reading one replication batch, and applying one committed batch SHALL NOT grow with the total number of entries already in the log.

#### Scenario: Append cost is independent of log size

- **GIVEN** a log holding 1,000 entries and a log holding 100,000 entries
- **WHEN** one entry is appended to each
- **THEN** the append performs a constant number of index and buffer operations in both cases

#### Scenario: Apply cost follows the batch

- **GIVEN** a server applying a batch of 100 entries at log index 50,000
- **WHEN** it applies the batch
- **THEN** the work is proportional to 100 entries, not to 50,000
