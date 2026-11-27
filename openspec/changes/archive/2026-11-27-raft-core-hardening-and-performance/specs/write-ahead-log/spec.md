# Spec Delta

## MODIFIED Requirements

### Requirement: Exact and linear-time recovery

Recovering a WAL SHALL decode each complete record once, SHALL verify each record's checksum, SHALL ignore an incomplete trailing record, and SHALL return entries in ascending index order. Recovery SHALL stop at the last valid record when a checksum fails, a payload is empty, or a record length is implausible.

#### Scenario: Large log recovers once

- **GIVEN** a WAL containing 10,000 complete records
- **WHEN** recovery runs
- **THEN** each record is decoded once and the returned entries are ordered by index

#### Scenario: Torn trailing write ignored

- **GIVEN** a WAL whose final record is truncated mid-payload
- **WHEN** recovery runs
- **THEN** all complete records are returned and the partial record is discarded

#### Scenario: Checksum mismatch stops recovery

- **GIVEN** a WAL whose record at index 50 has a valid length but a corrupted payload
- **WHEN** recovery runs
- **THEN** entries through index 49 are returned and the corrupted record is not accepted

#### Scenario: Empty or oversized record rejected

- **GIVEN** a WAL whose final header declares an empty payload or a length beyond the configured limit
- **WHEN** recovery runs
- **THEN** recovery completes without crashing and ignores that record

### Requirement: Truncation semantics

The WAL SHALL remove entries up to and including an index on prefix truncation, and SHALL remove entries at or after an index on suffix truncation, with the change durable and physically reflected in the WAL files across restart.

#### Scenario: Prefix truncation after snapshot

- **GIVEN** a WAL holding entries 1 through 50
- **WHEN** entries through index 40 are truncated
- **THEN** recovery after restart returns only entries 41 through 50

#### Scenario: Suffix truncation removes conflicts

- **GIVEN** a WAL holding entries 1 through 10
- **WHEN** entries at or after index 6 are truncated
- **THEN** recovery after restart returns only entries 1 through 5

#### Scenario: Disk usage reflects truncation

- **GIVEN** a WAL holding 10,000 records
- **WHEN** entries through 9,900 are truncated and the WAL is synced
- **THEN** the remaining WAL bytes no longer contain the removed records

## ADDED Requirements

### Requirement: Atomic truncation rewrite

When truncation requires rewriting the WAL, the system SHALL write the retained records to a temporary file, sync the file, atomically rename it over the old WAL, and sync the containing directory. A crash during the rewrite SHALL leave either the complete old WAL or the complete new WAL.

#### Scenario: Crash mid-rewrite preserves the log

- **GIVEN** a WAL rewrite in progress
- **WHEN** the process crashes before the rename
- **THEN** restart recovers the complete pre-rewrite WAL and no acknowledged entry is lost

#### Scenario: Rename is durable

- **GIVEN** a completed rewrite and rename
- **WHEN** the process crashes immediately afterward
- **THEN** restart recovers the rewritten WAL, not the old one

### Requirement: WAL rotation and bounded size

The WAL SHALL rotate to a new file when the active file reaches a configured maximum size, SHALL preserve record order across files, and SHALL recover entries from all retained files in index order.

#### Scenario: Rotation creates a new file

- **GIVEN** a WAL with a configured maximum file size and an active file at that size
- **WHEN** another batch is appended
- **THEN** a new file is started and the batch is written there

#### Scenario: Recovery spans rotated files

- **GIVEN** a WAL rotated across three files
- **WHEN** recovery runs
- **THEN** entries from all three files are returned once each in ascending index order

### Requirement: WAL configuration contract

The WAL SHALL honor the configured sync method (`:fsync`, `:datasync`, or `:none`), maximum batch size, checksum setting, and maximum file size under the documented configuration keys, and SHALL NOT silently substitute a different sync method.

#### Scenario: Configured sync method applied

- **GIVEN** a WAL configured with `:fsync`
- **WHEN** a synchronous append completes
- **THEN** the file is synced with `fsync` rather than `datasync`

#### Scenario: Configured batch size applied

- **GIVEN** a WAL configured with a maximum batch size of 64
- **WHEN** 100 single-entry appends are queued
- **THEN** the buffer never exceeds 64 entries and flushes at that bound

### Requirement: WAL error propagation

A WAL write, sync, truncate, or rotation failure SHALL be reported to the caller, SHALL NOT be swallowed or converted to success, and SHALL leave the WAL in a state from which recovery can proceed.

#### Scenario: Sync failure reported

- **GIVEN** a WAL whose sync returns an error
- **WHEN** a synchronous append runs
- **THEN** the caller receives an error and the entries are not acknowledged as durable

#### Scenario: Failure does not wedge the writer

- **GIVEN** a WAL that experienced a transient write failure
- **WHEN** the next append runs after the failure clears
- **THEN** the WAL accepts the append and the server does not remain permanently blocked

### Requirement: Unique server identity

The system SHALL assign every cluster member a distinct uid derived from the cluster and the server id, so each member owns an independent WAL file, metadata row, data directory, and registry entry.

#### Scenario: Servers in one cluster get distinct uids

- **GIVEN** a cluster started with three server ids on one node
- **WHEN** the servers start
- **THEN** each has a different uid and a different WAL file

#### Scenario: Restart preserves identity

- **GIVEN** a server with a derived uid
- **WHEN** it restarts from the same data directory
- **THEN** it recovers the same uid and the same WAL

#### Scenario: Colliding uid rejected

- **GIVEN** a start request whose uid already belongs to a different server
- **WHEN** the server starts
- **THEN** startup fails with an identity error instead of sharing the WAL
