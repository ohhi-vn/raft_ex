# Spec Delta

## Purpose

Controls which servers belong to a cluster and whether each member votes, allowing operators to add, remove, promote, and demote members without corrupting quorum.

## ADDED Requirements

### Requirement: Member roles

Every cluster member SHALL have a membership role of voter, promotable, or non-voter, and a member added without an explicit role SHALL become a voter.

#### Scenario: Default join is a voter

- **GIVEN** a leader accepting a join request without a role
- **WHEN** the membership change commits
- **THEN** the new member is recorded as a voter

#### Scenario: Non-voting member added

- **GIVEN** a leader accepting a join request with the non-voter role
- **WHEN** the membership change commits
- **THEN** the member receives log entries but does not count toward elections or commit quorum

### Requirement: Role-aware join

The system SHALL accept a join request carrying an explicit role and SHALL reject a join for a server that is already a member.

#### Scenario: Duplicate join rejected

- **GIVEN** a cluster that already contains server C
- **WHEN** a client asks to add server C again
- **THEN** the request returns an already-member error and the cluster is unchanged

#### Scenario: Join requires leadership

- **GIVEN** a follower
- **WHEN** a client asks it to add a member
- **THEN** the request is redirected to the leader or fails with a no-leader error

### Requirement: Member removal

The system SHALL remove a member through a replicated cluster change and SHALL reject removal of an unknown member.

#### Scenario: Member removed

- **GIVEN** a cluster containing server C
- **WHEN** the leader commits a change removing C
- **THEN** C no longer appears in membership results and no longer counts toward quorum

#### Scenario: Unknown member removal rejected

- **GIVEN** a leader whose cluster does not contain server X
- **WHEN** a client asks to remove X
- **THEN** the request returns a not-member error and the cluster is unchanged

### Requirement: Promotion requires catch-up

Promoting a promotable or non-voter member to voter SHALL require that the member's replicated index is at least the leader's commit index.

#### Scenario: Caught-up member promoted

- **GIVEN** a non-voter whose replicated index equals the leader's commit index
- **WHEN** the leader promotes it
- **THEN** the member becomes a voter and counts toward future quorums

#### Scenario: Lagging member not promoted

- **GIVEN** a non-voter whose replicated index is behind the leader's commit index
- **WHEN** the leader is asked to promote it
- **THEN** the request returns a not-caught-up error and the role is unchanged

### Requirement: Demotion

The system SHALL allow a voter to be demoted to non-voter through a replicated cluster change and SHALL reject demotion when it would leave no voters.

#### Scenario: Voter demoted

- **GIVEN** a cluster with three voters
- **WHEN** the leader demotes one voter
- **THEN** that member stops counting toward quorum and the remaining two still form a majority

#### Scenario: Last voter cannot be demoted

- **GIVEN** a cluster with one voter
- **WHEN** the leader is asked to demote it
- **THEN** the request is rejected and the member remains a voter

### Requirement: Membership information

`members/2` SHALL return every member id, `members_info/2` SHALL return each member's role and replication progress, and `initial_members/2` SHALL return the configured initial member ids.

#### Scenario: Roles reported

- **GIVEN** a cluster with one voter, one promotable member, and one non-voter
- **WHEN** a client requests member information
- **THEN** each member's reported role matches its configured role

### Requirement: Serialized cluster changes

The leader SHALL allow only one uncommitted cluster change at a time and SHALL reject further changes until the pending change commits.

#### Scenario: Concurrent change rejected

- **GIVEN** a leader with an uncommitted cluster change
- **WHEN** another membership change is requested
- **THEN** the request returns a cluster-change-not-permitted error and the pending change continues
