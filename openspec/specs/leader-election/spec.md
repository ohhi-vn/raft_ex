# Leader Election Specification

## Purpose

Elects one leader for a multi-server cluster with pre-vote protection, durable term and vote state, and a safe hand-off when leadership is transferred.

## Requirements

### Requirement: Voter-only elections

The system SHALL count only members with voter membership toward election quorums and SHALL NOT send vote requests to non-voting members.

#### Scenario: Non-voters do not affect the majority

- **GIVEN** a cluster with two voters and one non-voter
- **WHEN** a candidate requests votes
- **THEN** the majority is two and the non-voter does not receive a vote request

#### Scenario: Single voter wins alone

- **GIVEN** a cluster with exactly one voter
- **WHEN** that voter starts an election
- **THEN** it wins without sending vote requests

### Requirement: Pre-vote before term change

A server SHALL run a pre-vote round before starting a real election and SHALL NOT increment its term unless a majority of voters grant the pre-vote.

#### Scenario: Failed pre-vote preserves the term

- **GIVEN** a follower whose election timer expires and whose peers reject the pre-vote
- **WHEN** the pre-vote round completes
- **THEN** the server remains a follower and its term is unchanged

#### Scenario: Successful pre-vote starts an election

- **GIVEN** a follower whose pre-vote is granted by a majority of voters
- **WHEN** the pre-vote round completes
- **THEN** the server becomes a candidate, increments its term, and requests votes

### Requirement: Request-vote handling

A server SHALL grant at most one vote per term and SHALL grant it only to a candidate whose log is at least as up to date as its own.

#### Scenario: Vote granted once per term

- **GIVEN** a voter that has already voted for candidate A in term 4
- **WHEN** candidate B requests a vote in term 4
- **THEN** the voter denies the request and does not change its vote

#### Scenario: Stale candidate denied

- **GIVEN** a voter in term 5
- **WHEN** a candidate in term 3 requests a vote
- **THEN** the voter denies the request and reports term 5

#### Scenario: Out-of-date log denied

- **GIVEN** a candidate whose last log term is lower than the voter's last log term
- **WHEN** the candidate requests a vote with a higher term
- **THEN** the voter denies the vote and does not adopt the candidate's term

### Requirement: Durable term and vote

The system SHALL persist `current_term` and `voted_for` before acting on a term change or sending a vote, and SHALL restore both after restart.

#### Scenario: Term survives restart

- **GIVEN** a server that adopted term 7 and voted for candidate A
- **WHEN** the server restarts
- **THEN** it reports term 7, has the same vote recorded, and does not vote twice in term 7

#### Scenario: Candidate persists before requesting votes

- **GIVEN** a server starting a candidate election for term 5
- **WHEN** it sends vote requests
- **THEN** term 5 and its self-vote are already durable

### Requirement: Election timeout handling

A follower SHALL use a randomized election timeout between the configured minimum and maximum and SHALL reset that timeout whenever it accepts leader traffic.

#### Scenario: Leader traffic prevents elections

- **GIVEN** a follower with an election timer about to expire
- **WHEN** it accepts an AppendEntries or Heartbeat from the current leader
- **THEN** the timer is reset and the follower does not start an election

#### Scenario: Expired timer starts pre-vote

- **GIVEN** a follower that has received no leader traffic before its timeout
- **WHEN** the timeout expires
- **THEN** the follower starts a pre-vote round

### Requirement: Leader no-op entry

A newly elected leader SHALL append a no-op entry in its own term and SHALL permit cluster changes only after that entry is committed.

#### Scenario: Prior-term entries commit after election

- **GIVEN** a new leader whose log contains uncommitted entries from an earlier term
- **WHEN** the no-op entry is replicated to a majority
- **THEN** the earlier entries are committed and applied

#### Scenario: Cluster changes wait for the no-op

- **GIVEN** a newly elected leader whose no-op entry is not yet committed
- **WHEN** a client requests a membership change
- **THEN** the leader rejects the request as not permitted yet

### Requirement: Leadership transfer

`transfer_leadership/3` SHALL ask a caught-up voter to start an election immediately and SHALL make the old leader step down once it observes the target's higher term.

#### Scenario: Transfer moves leadership

- **GIVEN** a leader and a caught-up voter target
- **WHEN** leadership transfer to the target succeeds
- **THEN** the target becomes leader in a higher term and the old leader becomes a follower

#### Scenario: Invalid target rejected

- **GIVEN** a leader
- **WHEN** leadership transfer is requested for itself, an unknown server, or a server that is not caught up
- **THEN** the request returns an error and the current leader remains in office
