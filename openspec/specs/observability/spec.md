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

A metric update SHALL write to the ETS-backed store from the calling process and SHALL NOT require a call or cast to a metrics server.

#### Scenario: Hot path does not queue on a process

- **GIVEN** a server applying committed entries
- **WHEN** it records a commit metric
- **THEN** the update completes without sending a message to another process

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
