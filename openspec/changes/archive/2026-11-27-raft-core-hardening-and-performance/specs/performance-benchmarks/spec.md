# Spec Delta

## MODIFIED Requirements

### Requirement: Benchmark scenarios

The suite SHALL measure WAL append, log fold, committed-entry apply, WAL recovery, replication batch construction, query handling, end-to-end command processing, follower catch-up, WAL truncation, and snapshot transfer, and SHALL report each scenario separately.

#### Scenario: All scenarios run

- **GIVEN** a clean checkout with dependencies installed
- **WHEN** the benchmark suite runs without arguments
- **THEN** every scenario executes and a result row is printed for each one

#### Scenario: Scenario selection

- **GIVEN** a caller who names one scenario
- **WHEN** the suite runs
- **THEN** only that scenario executes

### Requirement: Regression comparison

The suite SHALL support comparing a run against a recorded baseline, SHALL report the percentage change per scenario, SHALL verify that each scenario used its configured input, and SHALL exit non-zero when a scenario exceeds a configurable regression threshold.

#### Scenario: Improvement reported

- **GIVEN** a baseline and a run whose WAL append throughput is higher
- **WHEN** comparison runs
- **THEN** the report shows a positive throughput change for that scenario and the suite exits successfully

#### Scenario: Regression fails the run

- **GIVEN** a baseline and a run whose recovery time exceeds the configured threshold
- **WHEN** comparison runs
- **THEN** the report marks the scenario as a regression and the suite exits non-zero

#### Scenario: Misconfigured scenario fails

- **GIVEN** a scenario configured to build 100-entry batches
- **WHEN** the implementation builds one-entry batches
- **THEN** the scenario reports a configuration error and the suite exits non-zero

### Requirement: Documented results

Recorded baseline results SHALL be documented alongside the command used to produce them, the machine and runtime versions, and the source revision, and the documentation SHALL explain any scenario that regressed.

#### Scenario: Results are traceable

- **GIVEN** a recorded baseline
- **WHEN** a reader opens the performance documentation
- **THEN** the command, machine, runtime versions, source revision, and result table are present

#### Scenario: Regression is explained

- **GIVEN** a baseline comparison in which one scenario regressed
- **WHEN** a reader opens the performance documentation
- **THEN** the regression is called out with its cause or an explicit follow-up

## ADDED Requirements

### Requirement: End-to-end command benchmark

The suite SHALL measure command processing from client submission through durable append, replication, commit, and reply, for a single server and for three- and five-server clusters, with sequential and concurrent clients.

#### Scenario: Single-server command latency

- **GIVEN** a single-server cluster
- **WHEN** one client submits commands sequentially
- **THEN** the suite reports throughput and per-command latency for the full path

#### Scenario: Concurrent clients

- **GIVEN** a three-server cluster
- **WHEN** multiple clients submit commands concurrently
- **THEN** the suite reports throughput and latency under concurrency

### Requirement: Follower catch-up benchmark

The suite SHALL measure how long an idle leader takes to bring a lagging follower from a known index to the leader's last index without new client commands.

#### Scenario: Lagging follower catches up

- **GIVEN** a leader with 10,000 entries and an idle client
- **WHEN** a follower starts at index 1
- **THEN** the suite reports the time and batches needed to reach the leader's last index

### Requirement: WAL truncation benchmark

The suite SHALL measure prefix and suffix truncation on a large WAL, SHALL verify the file size after truncation, and SHALL verify recovery returns only the retained entries after a restart.

#### Scenario: Truncation measured and verified

- **GIVEN** a WAL with 100,000 records
- **WHEN** the truncation scenario runs
- **THEN** it reports truncation latency, the resulting file size, and a successful recovery of the retained range

### Requirement: Snapshot transfer benchmark

The suite SHALL measure snapshot transfer for a snapshot larger than one chunk, SHALL exercise the first, middle, and last chunk paths, and SHALL verify the follower installs the complete snapshot.

#### Scenario: Multi-chunk transfer measured

- **GIVEN** a snapshot larger than the configured chunk size
- **WHEN** the snapshot transfer scenario runs
- **THEN** it reports transfer time and chunk count, and the follower's state matches the leader's snapshot
