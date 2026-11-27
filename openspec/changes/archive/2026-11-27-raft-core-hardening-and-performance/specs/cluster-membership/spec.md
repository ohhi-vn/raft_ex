# Spec Delta

## MODIFIED Requirements

### Requirement: Member removal

The system SHALL remove a member through a replicated cluster change and SHALL reject removal of an unknown member. After removal, the leader SHALL ignore replication replies from that member and SHALL NOT re-add it to the membership view.

#### Scenario: Member removed

- **GIVEN** a cluster containing server C
- **WHEN** the leader commits a change removing C
- **THEN** C no longer appears in membership results and no longer counts toward quorum

#### Scenario: Unknown member removal rejected

- **GIVEN** a leader whose cluster does not contain server X
- **WHEN** a client asks to remove X
- **THEN** the request returns a not-member error and the cluster is unchanged

#### Scenario: Delayed reply from removed member ignored

- **GIVEN** a leader that removed server C while a replication request to C was in flight
- **WHEN** C's delayed reply arrives
- **THEN** the leader ignores it and C does not reappear in membership

## ADDED Requirements

### Requirement: Membership durability

Committed membership SHALL survive restart and snapshot restore, and a server SHALL restore the member set and roles recorded at its snapshot index or replayed from its log.

#### Scenario: Membership survives restart

- **GIVEN** a cluster that added member C and committed the change
- **WHEN** the cluster restarts from its data directories
- **THEN** C is still a member with the same role and still counts toward quorum

#### Scenario: Removed member stays removed

- **GIVEN** a cluster that removed member C and compacted
- **WHEN** a server restarts from the snapshot
- **THEN** C is not restored as a member

#### Scenario: Role survives restore

- **GIVEN** a cluster with a non-voter member D
- **WHEN** a server restores from a snapshot taken after D joined
- **THEN** D is still a non-voter and does not count toward quorum

### Requirement: Local role reflects membership

A server's local membership role SHALL reflect its committed cluster role after join, promotion, or demotion, and that role SHALL drive elections, quorum counting, and reporting.

#### Scenario: Non-voter knows it is a non-voter

- **GIVEN** a server added with the non-voter role
- **WHEN** the change commits
- **THEN** the server reports itself as a non-voter and does not campaign

#### Scenario: Promoted member becomes a voter

- **GIVEN** a caught-up non-voter
- **WHEN** it is promoted and the change commits
- **THEN** it reports itself as a voter and counts toward quorum

#### Scenario: Demoted member stops counting

- **GIVEN** a voter
- **WHEN** it is demoted and the change commits
- **THEN** it reports itself as a non-voter and stops counting toward quorum

### Requirement: Voter-preserving removal

A leave or removal that would leave the cluster with no voters SHALL be rejected, and a leader that removes itself SHALL step down after that change commits.

#### Scenario: Last voter leave rejected

- **GIVEN** a cluster with one voter
- **WHEN** that voter requests to leave
- **THEN** the request is rejected and the voter remains

#### Scenario: Leader self-removal steps down

- **GIVEN** a leader that commits a change removing itself
- **WHEN** the change applies
- **THEN** the leader steps down and no longer accepts commands
