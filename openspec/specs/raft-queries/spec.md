# Raft Queries Specification

## Purpose

Serves state-machine reads and auxiliary commands with explicit consistency levels so callers can choose local reads, leader reads, or linearizable reads through one API.

## Requirements

### Requirement: Local query

A local query SHALL return the state machine's value at the server's applied index without contacting other servers or requiring a leader.

#### Scenario: Follower answers locally

- **GIVEN** a follower with applied index 12
- **WHEN** a client issues a local query
- **THEN** the follower returns the value with index 12 and its own server id

#### Scenario: Local query does not redirect

- **GIVEN** a follower that knows the leader
- **WHEN** a client issues a local query to the follower
- **THEN** the follower answers directly rather than redirecting to the leader

### Requirement: Leader query

A leader query SHALL be answered by the current leader, and a server that is not the leader SHALL redirect the request to the leader it knows.

#### Scenario: Follower redirects to leader

- **GIVEN** a follower whose known leader is server B
- **WHEN** a client issues a leader query to the follower
- **THEN** the request is served by server B and the reply identifies B as leader

#### Scenario: No known leader

- **GIVEN** a server with no known leader
- **WHEN** a client issues a leader query
- **THEN** the request fails with a no-leader error

### Requirement: Consistent query

A consistent query SHALL be answered only after the leader confirms leadership with a majority of voters and has applied through the commit index observed when the query arrived. The query SHALL complete or fail within the caller's timeout and SHALL NOT wait without bound.

#### Scenario: Majority confirms leadership

- **GIVEN** a leader whose heartbeat is acknowledged by a majority of voters
- **WHEN** a consistent query arrives
- **THEN** the leader applies through its current commit index and returns the result with its server id

#### Scenario: No quorum

- **GIVEN** a leader that cannot obtain heartbeat acknowledgements from a majority
- **WHEN** a consistent query arrives
- **THEN** the query fails rather than returning a possibly stale value

#### Scenario: Cluster change in progress

- **GIVEN** a leader with an uncommitted cluster change
- **WHEN** a consistent query arrives
- **THEN** the query waits until the cluster change is applied before the leader confirms leadership

#### Scenario: Quorum never arrives

- **GIVEN** a leader that loses contact with the quorum while a consistent query is pending
- **WHEN** the query's deadline expires
- **THEN** the query fails with a timeout and the pending entry is removed

### Requirement: Query failure reporting

A query SHALL return a timeout carrying the server id when no reply arrives, SHALL never return a value from a server that cannot prove the requested consistency, and SHALL produce exactly one reply for every accepted query.

#### Scenario: Timeout reported

- **GIVEN** a server that does not answer a query before the caller's timeout
- **WHEN** the timeout expires
- **THEN** the caller receives a timeout result naming the server

#### Scenario: Inconsistent index does not crash

- **GIVEN** a consistent query whose required index is ahead of the leader's applied index
- **WHEN** the query is evaluated
- **THEN** the query waits or fails with an error instead of raising

### Requirement: Auxiliary command routing

Auxiliary commands SHALL be dispatched to the state machine's auxiliary handler in every server state, for both calls and casts, and `consistent_aux/3` SHALL wait for the same quorum confirmation as a consistent query.

#### Scenario: Aux call answered

- **GIVEN** a server whose state machine implements an auxiliary handler
- **WHEN** a client issues an auxiliary call
- **THEN** the server returns the handler's reply with its server id

#### Scenario: Aux cast does not reply

- **GIVEN** a server whose state machine implements an auxiliary handler
- **WHEN** a client issues an auxiliary cast
- **THEN** the handler runs and no reply is sent to the caller

#### Scenario: Missing auxiliary handler

- **GIVEN** a server whose state machine does not implement an auxiliary handler
- **WHEN** a client issues an auxiliary call
- **THEN** the server returns an auxiliary-handler-not-implemented error

### Requirement: Bounded pending queries

The server SHALL bound the number of pending consistent queries and SHALL expire each query at its deadline, so a stalled quorum cannot grow memory without limit or strand callers.

#### Scenario: Pending limit reached

- **GIVEN** a server whose pending consistent-query queue is at its configured limit
- **WHEN** another consistent query arrives
- **THEN** the new query is rejected with a busy error and the queue does not grow

#### Scenario: Expired query removed

- **GIVEN** a pending consistent query whose deadline has passed
- **WHEN** the next heartbeat reply is processed
- **THEN** the expired query is removed and its caller receives a timeout

### Requirement: Cluster-change gating

A consistent query or consistent auxiliary command SHALL wait while a cluster change is uncommitted, and SHALL confirm leadership only after that change is applied.

#### Scenario: Query waits during change

- **GIVEN** a leader with an uncommitted membership change
- **WHEN** a consistent query arrives
- **THEN** the query is queued and is not answered until the change applies

#### Scenario: Query proceeds after change

- **GIVEN** a queued consistent query and a membership change that just applied
- **WHEN** the leader confirms leadership
- **THEN** the query is answered from the applied state

### Requirement: Local state query

A local state query SHALL return the requested server state, including membership, indexes, and per-server metrics, without requiring consensus and without timing out.

#### Scenario: Member overview answered

- **GIVEN** a running server
- **WHEN** a caller requests member overview
- **THEN** the server returns its membership and replication progress

#### Scenario: Unknown state request rejected

- **GIVEN** a running server
- **WHEN** a caller requests an unsupported state spec
- **THEN** the server returns an explicit error instead of timing out
