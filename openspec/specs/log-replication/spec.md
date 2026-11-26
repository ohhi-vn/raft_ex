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

### Requirement: Bounded in-flight AppendEntries

The leader SHALL limit the number of unacknowledged AppendEntries requests in flight to a single follower to a configured pipeline depth, and SHALL NOT send a new request while that follower's in-flight count is at the limit.

#### Scenario: Request sent within depth

- **GIVEN** a follower with zero in-flight requests and a pipeline depth of 3
- **WHEN** the leader has entries to replicate
- **THEN** the leader sends an AppendEntries request and records it as in-flight

#### Scenario: Limit reached

- **GIVEN** a follower with 3 in-flight requests and a pipeline depth of 3
- **WHEN** the leader has additional entries to replicate
- **THEN** the leader does not send another request until an acknowledgement arrives

### Requirement: Out-of-order acknowledgement handling

The leader SHALL accept AppendEntries replies that arrive out of order, and SHALL advance a follower's match index only when a contiguous prefix of entries has been acknowledged by that follower.

#### Scenario: Later reply arrives first

- **GIVEN** two in-flight requests covering entries 1-5 and 6-10
- **WHEN** the reply for the second request arrives before the first
- **THEN** the leader records the second as acknowledged but does not advance match index past 5 until the first reply arrives

#### Scenario: Contiguous prefix advances match index

- **GIVEN** acknowledgements for entries 1-5 and 6-10 have both arrived
- **WHEN** the leader processes the acknowledgements
- **THEN** match index advances to 10

### Requirement: Rejection handling without blocking

When an in-flight AppendEntries request is rejected, the leader SHALL reduce the follower's next index using the reply's hint or by one, SHALL mark that request as resolved, and SHALL retry replication for the follower without waiting for the remaining in-flight requests to complete.

#### Scenario: Rejection frees the slot

- **GIVEN** a follower with 2 in-flight requests where one is rejected
- **WHEN** the rejection is processed
- **THEN** the in-flight count decreases and a new request may be sent

#### Scenario: Retry after rejection

- **GIVEN** a rejected request with a next-index hint of 6
- **WHEN** the leader retries replication
- **THEN** it sends entries starting at index 6

### Requirement: Backpressure under slow follower

When a follower's in-flight queue is full, the leader SHALL skip sending to that follower, SHALL record a `replication_backpressure` metric, and SHALL retry on the next tick or acknowledgement instead of blocking or growing unbounded memory.

#### Scenario: Skip and metric

- **GIVEN** a follower whose in-flight queue is full
- **WHEN** the leader attempts to replicate
- **THEN** no request is sent to that follower and the `replication_backpressure` counter is incremented

#### Scenario: Retry later

- **GIVEN** a follower that was skipped due to backpressure
- **WHEN** an acknowledgement arrives and frees a slot
- **THEN** the leader sends the pending entries

### Requirement: Adaptive batch sizing

The leader SHALL size each AppendEntries batch for a follower based on the follower's lag and the available pipeline capacity, so that a follower with more available capacity receives a larger batch and a follower at the limit receives a batch of at least one entry.

#### Scenario: Larger batch for lagging follower with capacity

- **GIVEN** a follower lagging by 100 entries with 3 of 3 slots free
- **WHEN** the leader builds a request
- **THEN** the batch size is greater than the minimum of one

#### Scenario: Minimum batch at limit

- **GIVEN** a follower with zero free pipeline slots
- **WHEN** a slot frees and the leader builds a request
- **THEN** the batch contains at least one entry
