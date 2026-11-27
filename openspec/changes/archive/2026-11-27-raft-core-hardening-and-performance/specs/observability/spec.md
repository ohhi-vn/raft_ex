# Spec Delta

## MODIFIED Requirements

### Requirement: Direct ETS updates

A metric update SHALL write to the ETS-backed store from the calling process, SHALL NOT require a call or cast to a metrics server, and SHALL NOT perform a table-existence check or table lookup on every update once the store is initialized.

#### Scenario: Hot path does not queue on a process

- **GIVEN** a server applying committed entries
- **WHEN** it records a commit metric
- **THEN** the update completes without sending a message to another process

#### Scenario: Initialization is not repeated

- **GIVEN** an initialized metric store
- **WHEN** a counter is incremented one million times
- **THEN** the update path performs only the counter operation and no repeated table initialization

## ADDED Requirements

### Requirement: Replication lag metric semantics

A replication lag metric SHALL report a value whose unit matches its name, and SHALL report the follower's current index gap or an explicitly computed time lag, never an index gap under a millisecond name.

#### Scenario: Index gap reported

- **GIVEN** a leader whose last index is 1,000 and a follower whose match index is 900
- **WHEN** the lag metric is read
- **THEN** it reports a lag of 100 in the unit declared by the metric name

#### Scenario: Caught-up follower reports zero

- **GIVEN** a follower whose match index equals the leader's last index
- **WHEN** the lag metric is read
- **THEN** it reports zero

#### Scenario: Metric name and unit agree

- **GIVEN** a metric named for milliseconds
- **WHEN** the implementation records an index gap
- **THEN** the metric is renamed or the value is converted so name and unit agree

### Requirement: Per-server counters

`key_metrics/2` and `member_overview/2` SHALL report real per-server values, including applied index, commit index, snapshot index, last log index, and membership role, instead of empty maps or stubs.

#### Scenario: Key metrics populated

- **GIVEN** a running server with applied entries
- **WHEN** a caller requests key metrics
- **THEN** the reply includes the server's applied, commit, snapshot, and last indexes and its role

#### Scenario: Member overview populated

- **GIVEN** a cluster with replication in progress
- **WHEN** a caller requests member overview
- **THEN** each member's replication progress is reported from live state
