# Performance Benchmarks Specification

## Purpose

Defines a repeatable benchmark suite that measures RaftEx hot paths, records comparable baselines, and reports regressions so performance work can be validated with evidence.

## Requirements

### Requirement: Benchmark scenarios

The suite SHALL measure WAL append, log fold, committed-entry apply, WAL recovery, replication batch construction, and query handling, and SHALL report each scenario separately.

#### Scenario: All scenarios run

- **GIVEN** a clean checkout with dependencies installed
- **WHEN** the benchmark suite runs without arguments
- **THEN** every scenario executes and a result row is printed for each one

#### Scenario: Scenario selection

- **GIVEN** a caller who names one scenario
- **WHEN** the suite runs
- **THEN** only that scenario executes

### Requirement: Isolated runs

Each benchmark run SHALL use a fresh temporary data directory and SHALL remove it afterward, so results are not affected by prior runs.

#### Scenario: Repeated runs start clean

- **GIVEN** a completed benchmark run
- **WHEN** the suite runs again
- **THEN** it starts with empty WAL and snapshot directories

### Requirement: Stable measurement

The suite SHALL perform warm-up iterations before measuring, SHALL use a fixed input size and seed, and SHALL report the iteration count, elapsed time, and a throughput or latency figure for each scenario.

#### Scenario: Reported figures are reproducible

- **GIVEN** two runs on the same machine with the same arguments
- **WHEN** their reports are compared
- **THEN** each scenario reports the same iteration count and input size, and throughput figures are within normal run-to-run variance

### Requirement: Baseline recording

The suite SHALL write results to a versioned baseline file containing the scenario name, metrics, input size, timestamp, and the source revision.

#### Scenario: Baseline written

- **GIVEN** a benchmark run with baseline recording enabled
- **WHEN** the run completes
- **THEN** the baseline file contains one entry per scenario with its metrics and the current source revision

### Requirement: Regression comparison

The suite SHALL support comparing a run against a recorded baseline, SHALL report the percentage change per scenario, and SHALL exit non-zero when a scenario exceeds a configurable regression threshold.

#### Scenario: Improvement reported

- **GIVEN** a baseline and a run whose WAL append throughput is higher
- **WHEN** comparison runs
- **THEN** the report shows a positive throughput change for that scenario and the suite exits successfully

#### Scenario: Regression fails the run

- **GIVEN** a baseline and a run whose recovery time exceeds the configured threshold
- **WHEN** comparison runs
- **THEN** the report marks the scenario as a regression and the suite exits non-zero

### Requirement: Documented results

Recorded baseline results SHALL be documented alongside the command used to produce them and the machine and runtime versions used.

#### Scenario: Results are traceable

- **GIVEN** a recorded baseline
- **WHEN** a reader opens the performance documentation
- **THEN** the command, machine, runtime versions, and result table are present
