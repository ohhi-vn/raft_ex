# Spec Delta

## MODIFIED Requirements

### Requirement: Durable term and vote

The system SHALL persist `current_term` and `voted_for` before acting on a term change or sending a vote, and SHALL restore both after restart. A message carrying the current term SHALL NOT modify `voted_for`, and a persistence failure SHALL prevent the server from voting or replying as if the state were durable.

#### Scenario: Term survives restart

- **GIVEN** a server that adopted term 7 and voted for candidate A
- **WHEN** the server restarts
- **THEN** it reports term 7, has the same vote recorded, and does not vote twice in term 7

#### Scenario: Candidate persists before requesting votes

- **GIVEN** a server starting a candidate election for term 5
- **WHEN** it sends vote requests
- **THEN** term 5 and its self-vote are already durable

#### Scenario: Same-term traffic preserves the vote

- **GIVEN** a voter that voted for candidate A in term 4
- **WHEN** it receives a heartbeat or snapshot from a term-4 leader
- **THEN** its term remains 4 and its vote for A remains recorded

#### Scenario: Persistence failure blocks the vote

- **GIVEN** a voter whose term or vote cannot be persisted
- **WHEN** it is asked to grant a vote
- **THEN** it does not grant the vote and reports an error instead of acknowledging

### Requirement: Leadership transfer

`transfer_leadership/3` SHALL ask a caught-up voter to start an election immediately and SHALL make the old leader step down once it observes the target's higher term. The target SHALL persist its new term and vote before campaigning, and the old leader SHALL persist its observed term before acting as a follower.

#### Scenario: Transfer moves leadership

- **GIVEN** a leader and a caught-up voter target
- **WHEN** leadership transfer to the target succeeds
- **THEN** the target becomes leader in a higher term and the old leader becomes a follower

#### Scenario: Invalid target rejected

- **GIVEN** a leader
- **WHEN** leadership transfer is requested for itself, an unknown server, or a server that is not caught up
- **THEN** the request returns an error and the current leader remains in office

#### Scenario: Transfer term is durable

- **GIVEN** a successful leadership transfer
- **WHEN** the new leader and the old leader restart
- **THEN** both report the transferred term and the old leader does not campaign in the old term

## ADDED Requirements

### Requirement: Election effect application

The server runtime SHALL apply election outcomes that change term or role, including adopting a higher term, clearing the vote for a new term, and becoming a follower, before processing further messages.

#### Scenario: Candidate adopts higher term

- **GIVEN** a candidate in term 4
- **WHEN** a peer replies with term 6
- **THEN** the candidate persists term 6, clears its vote, and becomes a follower

#### Scenario: Pre-vote candidate adopts higher term

- **GIVEN** a server in a pre-vote round for term 4
- **WHEN** a peer replies with term 6
- **THEN** the server persists term 6 and leaves the pre-vote round as a follower

### Requirement: TimeoutNow authorization

A server SHALL accept a TimeoutNow request only from the leader it currently recognizes in the same term, and SHALL persist the resulting term before starting an election.

#### Scenario: Current leader triggers election

- **GIVEN** a follower whose recognized leader is server B in term 5
- **WHEN** server B sends TimeoutNow in term 5
- **THEN** the follower persists its new term and starts an election

#### Scenario: Non-leader request rejected

- **GIVEN** a follower whose recognized leader is server B
- **WHEN** server C sends TimeoutNow
- **THEN** the follower rejects the request and does not start an election

### Requirement: Leader step-down on higher term

A leader that receives any RPC or reply carrying a higher term SHALL persist that term, become a follower, and process the message under follower rules.

#### Scenario: Higher-term vote request

- **GIVEN** a leader in term 5
- **WHEN** it receives a RequestVote for term 6
- **THEN** it persists term 6, becomes a follower, and evaluates the vote

#### Scenario: Higher-term AppendEntries

- **GIVEN** a leader in term 5
- **WHEN** it receives AppendEntries from a term-6 leader
- **THEN** it persists term 6 and accepts the entries as a follower

#### Scenario: Higher-term reply

- **GIVEN** a leader in term 5
- **WHEN** a follower reply reports term 6
- **THEN** the leader persists term 6 and becomes a follower

### Requirement: Voter-only campaigning

Only voters SHALL start elections, count a self-vote, or count themselves toward commit quorum. A non-voter SHALL remain a follower, receive log entries, and never campaign.

#### Scenario: Non-voter election timeout

- **GIVEN** a non-voter whose election timer expires
- **WHEN** the timer fires
- **THEN** it does not start a pre-vote or election

#### Scenario: Non-voter cannot win alone

- **GIVEN** a cluster with two voters and one non-voter
- **WHEN** the non-voter campaigns
- **THEN** it cannot become leader and does not count its own vote

#### Scenario: Non-voter leader impossible

- **GIVEN** a non-voter that has received every log entry
- **WHEN** it considers its replication progress
- **THEN** that progress is not counted toward the commit quorum

### Requirement: Pre-vote integrity

A pre-vote SHALL NOT persist a vote for the current term, and a pre-vote reply SHALL be counted only for the round token that issued it.

#### Scenario: Failed pre-vote leaves the vote unchanged

- **GIVEN** a voter with no vote recorded in term 4
- **WHEN** its pre-vote fails
- **THEN** no vote is recorded for term 4 and it can still grant a legitimate vote

#### Scenario: Stale pre-vote reply ignored

- **GIVEN** a server in pre-vote round with token T2
- **WHEN** a reply from an earlier round with token T1 arrives
- **THEN** the reply is ignored and does not count toward the majority

### Requirement: Check-quorum step-down

A leader that cannot obtain an acknowledgement from a majority of voters within the election timeout SHALL step down to follower instead of continuing to accept commands.

#### Scenario: Majority lost

- **GIVEN** a leader that loses contact with the majority of voters
- **WHEN** the election timeout elapses without a majority acknowledgement
- **THEN** the leader steps down and stops accepting commands

#### Scenario: Majority restored before timeout

- **GIVEN** a leader that briefly loses contact with the majority
- **WHEN** a majority acknowledges before the election timeout
- **THEN** the leader remains in office
