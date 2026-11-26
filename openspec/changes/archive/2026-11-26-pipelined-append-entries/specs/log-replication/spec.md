# Spec Delta

## ADDED Requirements

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
