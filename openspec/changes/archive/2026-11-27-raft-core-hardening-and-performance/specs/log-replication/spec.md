# Spec Delta

## MODIFIED Requirements

### Requirement: Batch-proportional replication work

Replication SHALL read only the entries in the requested AppendEntries range, SHALL write the acceptable entries of one AppendEntries request in a single WAL write, and SHALL apply only the newly committed index range. A follower that cannot durably store the batch SHALL be treated as having rejected it.

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

#### Scenario: Failed write rejects the batch

- **GIVEN** a follower that cannot write an AppendEntries batch durably
- **WHEN** it processes the request
- **THEN** it replies failure and does not report the entries as stored

### Requirement: Bounded in-flight AppendEntries

The leader SHALL limit the number of unacknowledged AppendEntries requests in flight to a single follower to a configured pipeline depth, SHALL NOT send a new request while that follower's in-flight count is at the limit, and SHALL keep acknowledgement bookkeeping bounded by the pipeline depth.

#### Scenario: Request sent within depth

- **GIVEN** a follower with zero in-flight requests and a pipeline depth of 3
- **WHEN** the leader has entries to replicate
- **THEN** the leader sends an AppendEntries request and records it as in-flight

#### Scenario: Limit reached

- **GIVEN** a follower with 3 in-flight requests and a pipeline depth of 3
- **WHEN** the leader has additional entries to replicate
- **THEN** the leader does not send another request until an acknowledgement arrives

#### Scenario: Bookkeeping does not grow without bound

- **GIVEN** a leader that has processed one million acknowledgements from a follower
- **WHEN** it records another acknowledgement
- **THEN** the acknowledgement state remains proportional to the pipeline depth, not to the number of acknowledgements seen

### Requirement: Out-of-order acknowledgement handling

The leader SHALL accept AppendEntries replies that arrive out of order, and SHALL advance a follower's match index only when a contiguous prefix of entries has been acknowledged by that follower. Advancing match index SHALL take amortized constant time per reply.

#### Scenario: Later reply arrives first

- **GIVEN** two in-flight requests covering entries 1-5 and 6-10
- **WHEN** the reply for the second request arrives before the first
- **THEN** the leader records the second as acknowledged but does not advance match index past 5 until the first reply arrives

#### Scenario: Contiguous prefix advances match index

- **GIVEN** acknowledgements for entries 1-5 and 6-10 have both arrived
- **WHEN** the leader processes the acknowledgements
- **THEN** match index advances to 10

#### Scenario: Stale acknowledgement ignored

- **GIVEN** a follower whose match index is 10
- **WHEN** a delayed reply for entries 1-5 arrives
- **THEN** the match index remains 10 and the reply does not re-enter the acknowledgement set

### Requirement: Adaptive batch sizing

The leader SHALL size each AppendEntries batch for a follower up to the configured maximum batch size based on the follower's lag and the available pipeline capacity, so a follower with more available capacity receives a larger batch and a follower at the limit receives a batch of at least one entry.

#### Scenario: Larger batch for lagging follower with capacity

- **GIVEN** a follower lagging by 100 entries with 3 of 3 slots free
- **WHEN** the leader builds a request
- **THEN** the batch size is greater than the minimum of one

#### Scenario: Minimum batch at limit

- **GIVEN** a follower with zero free pipeline slots
- **WHEN** a slot frees and the leader builds a request
- **THEN** the batch contains at least one entry

#### Scenario: Configured maximum respected

- **GIVEN** a configured maximum AppendEntries batch size of 128 and a follower lagging by 1,000 entries
- **WHEN** the leader builds a request with free capacity
- **THEN** the request contains at most 128 entries

## ADDED Requirements

### Requirement: Catch-up without client traffic

The leader SHALL continue replicating to a lagging follower when no new client commands arrive, by scheduling further batches when acknowledgements free pipeline capacity.

#### Scenario: Idle leader catches up a follower

- **GIVEN** a leader with 10,000 committed entries, an idle client, and a follower whose next index is 1
- **WHEN** replication runs
- **THEN** the follower advances in successive batches until its match index reaches the leader's last index

### Requirement: Non-blocking peer communication

Sending to a peer SHALL NOT sleep or block the consensus process, SHALL use non-suspending sends where available, and SHALL isolate a slow or unreachable peer so other peers continue to make progress.

#### Scenario: Partitioned peer does not stall others

- **GIVEN** a leader with three followers, one of which is unreachable
- **WHEN** the leader replicates
- **THEN** the reachable followers continue to receive batches while the unreachable peer is retried separately

#### Scenario: Full distribution buffer reports backpressure

- **GIVEN** a peer whose distribution buffer cannot accept another message
- **WHEN** the leader sends to it
- **THEN** the send reports backpressure and the leader schedules a retry without blocking

### Requirement: Current-term commit advancement

The leader SHALL advance `commit_index` only for an entry from its current term and SHALL NOT commit an earlier-term entry by counting replicas alone. The AppendEntries reply path SHALL apply the same rule as the local command path.

#### Scenario: Prior-term entry not committed by count

- **GIVEN** a new leader with an uncommitted entry from an earlier term replicated to a majority
- **WHEN** the leader processes the acknowledgements
- **THEN** it does not advance `commit_index` past that entry until a current-term entry commits

#### Scenario: Current-term entry commits prior entries

- **GIVEN** a new leader whose no-op entry from its own term reaches a majority
- **WHEN** the no-op commits
- **THEN** earlier entries up to the no-op also commit and apply

### Requirement: Monotonic commit index

`commit_index` SHALL never decrease, and a replication, membership, or snapshot event that would compute a lower value SHALL leave the current value unchanged.

#### Scenario: Peer removal does not lower commit

- **GIVEN** a leader with commit index 100
- **WHEN** a peer removal changes the computed majority index to 90
- **THEN** `commit_index` remains 100

#### Scenario: Snapshot does not lower commit

- **GIVEN** a server with commit index 100 and a snapshot at index 40
- **WHEN** it installs the snapshot
- **THEN** `commit_index` remains at least 100

### Requirement: Backtracking hint safety

When a follower rejects AppendEntries, the leader SHALL lower that follower's next index using the reply hint or by one, and SHALL NOT move next index forward. A hint greater than the current next index SHALL be ignored.

#### Scenario: Divergent longer suffix does not advance next index

- **GIVEN** a follower with a longer divergent suffix whose hint is above the leader's next index
- **WHEN** the leader processes the rejection
- **THEN** it keeps or lowers next index and retries from a valid index

#### Scenario: Stale hint ignored

- **GIVEN** a leader whose next index for a follower is 10
- **WHEN** a delayed rejection carries a hint of 15
- **THEN** the leader does not set next index to 15

### Requirement: Tick replication obeys pipeline limits

Periodic tick replication SHALL use the same pipeline, backpressure, snapshot-status, and next-index bookkeeping as command-driven replication.

#### Scenario: Tick with full pipeline sends nothing

- **GIVEN** a follower with a full in-flight pipeline
- **WHEN** the leader tick fires
- **THEN** no AppendEntries is sent to that follower

#### Scenario: Tick during snapshot sends no append

- **GIVEN** a follower whose status is sending a snapshot
- **WHEN** the leader tick fires
- **THEN** no AppendEntries is sent to that follower until the snapshot completes
