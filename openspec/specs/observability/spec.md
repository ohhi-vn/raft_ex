# Observability Specification

## Purpose

Defines how RaftEx records runtime metrics so that concurrent servers update them without losing counts and without adding a message round-trip to consensus hot paths.

## Requirements

### Requirement: Atomic counter updates

Counter increments SHALL be atomic, and the final value SHALL equal the sum of all increments even when many processes update the same counter concurrently.

#### Scenario: Concurrent increments are not lost

- **GIVEN** a counter starting at zero
- **WHEN** 100 processes each increment it 100 times concurrently
- **THEN** the counter reads 10,000

#### Scenario: Increment by a positive amount

- **GIVEN** a counter with value 7
- **WHEN** it is incremented by 3
- **THEN** it reads 10

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

### Requirement: Metric store initialization

A metric update SHALL succeed whether or not the metrics table already exists, and concurrent first updates SHALL NOT fail or create duplicate tables.

#### Scenario: First update creates the table

- **GIVEN** no metrics table
- **WHEN** the first counter increment runs
- **THEN** the table is created and the counter reads the incremented value

#### Scenario: Concurrent first updates

- **GIVEN** no metrics table
- **WHEN** several processes increment counters at the same time
- **THEN** every update succeeds and no process crashes

### Requirement: Histogram accumulation

A histogram observation SHALL increment the observation count and add the value to the accumulated sum, and those aggregates SHALL remain correct under concurrent observations.

#### Scenario: Observations accumulate

- **GIVEN** a histogram with no observations
- **WHEN** values 5 and 15 are observed
- **THEN** the histogram reports a count of 2 and a sum of 20

#### Scenario: Concurrent observations

- **GIVEN** a histogram with no observations
- **WHEN** 50 processes each observe the value 2 concurrently
- **THEN** the histogram reports a count of 50 and a sum of 100

### Requirement: Metric reads

Reading a single metric SHALL return its latest stored value, and reading all metrics SHALL return a consistent snapshot of counters, gauges, and histograms.

#### Scenario: Read a gauge

- **GIVEN** a gauge set to 42
- **WHEN** it is read
- **THEN** the value 42 is returned

#### Scenario: Read all metrics

- **GIVEN** one counter, one gauge, and one histogram
- **WHEN** all metrics are read
- **THEN** the result contains each metric under its kind and name

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
