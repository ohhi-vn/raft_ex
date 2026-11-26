# Write-Ahead Log Specification

## Purpose

Defines how RaftEx durably records log entries before they are acknowledged, batches concurrent appends, recovers exactly what was written, and truncates safely.

## Requirements

### Requirement: Durable append acknowledgement

A synchronous append SHALL return success only after every entry in the request has been written to the server's WAL file and flushed according to the configured sync method.

#### Scenario: Acknowledged entries survive restart

- **GIVEN** a WAL with sync method `:datasync`
- **WHEN** a synchronous append returns success for entries 5 through 7
- **THEN** those entries are readable from the WAL file after the process restarts

#### Scenario: Write failure is reported

- **GIVEN** a WAL whose backing file cannot be written
- **WHEN** a synchronous append is attempted
- **THEN** the caller receives an error and the WAL does not report the entries as written

### Requirement: Group commit

When several synchronous appends are pending, the WAL SHALL write and sync them as one batch and SHALL acknowledge each caller only after that caller's entries are durable.

#### Scenario: Concurrent callers share one sync

- **GIVEN** three servers appending to the same WAL process concurrently
- **WHEN** the WAL flushes
- **THEN** all three requests are written in one batch and each caller is acknowledged after the batch is synced

#### Scenario: No caller acknowledged early

- **GIVEN** a caller whose entries are queued behind a larger pending batch
- **WHEN** the WAL begins writing the batch
- **THEN** the caller is not acknowledged until the write and sync complete

### Requirement: Bounded buffering

Buffering an append SHALL NOT copy the existing buffer, and the pending buffer SHALL be bounded by the configured maximum batch size.

#### Scenario: Buffer growth stays linear

- **GIVEN** an empty WAL buffer and a maximum batch size of 8192
- **WHEN** 1000 single-entry appends are buffered without a flush
- **THEN** total buffer work is proportional to 1000 entries and the buffer never exceeds 8192 entries

#### Scenario: Batch size triggers a flush

- **GIVEN** a buffer holding 8191 entries
- **WHEN** an append adds one more entry
- **THEN** the buffer is flushed and reset instead of growing past the configured maximum

### Requirement: Exact and linear-time recovery

Recovering a WAL SHALL decode each complete record once, SHALL ignore an incomplete trailing record, and SHALL return entries in ascending index order.

#### Scenario: Large log recovers once

- **GIVEN** a WAL containing 10,000 complete records
- **WHEN** recovery runs
- **THEN** each record is decoded once and the returned entries are ordered by index

#### Scenario: Torn trailing write ignored

- **GIVEN** a WAL whose final record is truncated mid-payload
- **WHEN** recovery runs
- **THEN** all complete records are returned and the partial record is discarded

### Requirement: Per-server WAL isolation

Each server uid SHALL own an independent WAL file, buffer, and index range, and appends for different uids SHALL NOT interleave in one file.

#### Scenario: Two servers keep separate logs

- **GIVEN** two servers with uids `a` and `b` appending to one WAL process
- **WHEN** both append entries with index 1
- **THEN** each uid's file contains only its own entry and each reports its own first and last index

### Requirement: Truncation semantics

The WAL SHALL remove entries up to and including an index on prefix truncation, and SHALL remove entries at or after an index on suffix truncation, with the change durable across restart.

#### Scenario: Prefix truncation after snapshot

- **GIVEN** a WAL holding entries 1 through 50
- **WHEN** entries through index 40 are truncated
- **THEN** recovery after restart returns only entries 41 through 50

#### Scenario: Suffix truncation removes conflicts

- **GIVEN** a WAL holding entries 1 through 10
- **WHEN** entries at or after index 6 are truncated
- **THEN** recovery after restart returns only entries 1 through 5

### Requirement: Range read

A WAL read SHALL return only buffered entries whose indexes fall within the requested inclusive range, in ascending index order.

#### Scenario: Read a subrange

- **GIVEN** a buffer holding entries 3 through 9
- **WHEN** entries 5 through 7 are read
- **THEN** exactly entries 5, 6, and 7 are returned in that order
