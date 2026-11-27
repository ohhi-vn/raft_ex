# Raft Startup Specification

## Purpose

Starts and configures a RaftEx system under the library's own OTP application, using its own environment and reporting startup failures instead of hiding them.

## Requirements

### Requirement: Application identity

Startup, shutdown, and configuration SHALL use the `raft_ex` OTP application and environment only, and SHALL NOT read, stop, load, or configure an application named `ra` or its persistent-term keys.

#### Scenario: Start does not touch Ra

- **GIVEN** an installed application named `ra` that is not running
- **WHEN** a caller invokes `RaftEx.start/1`
- **THEN** the `ra` application remains stopped and `raft_ex` is used for configuration

#### Scenario: Data directory from RaftEx environment

- **GIVEN** a `raft_ex` environment entry setting the data directory
- **WHEN** the default system starts
- **THEN** servers store data under that directory and not under a `ra` environment value

#### Scenario: Legacy Ra environment ignored

- **GIVEN** only a `ra` environment entry for the data directory
- **WHEN** the default system starts without a `raft_ex` data directory
- **THEN** the `ra` value is not used and the documented default is applied

### Requirement: Startup result

`RaftEx.start/1` and `RaftEx.start_in/1` SHALL start the default system and return `:ok` on success or `{:error, reason}` when the application or system cannot start.

#### Scenario: Successful start

- **GIVEN** a writable data directory
- **WHEN** a caller invokes `RaftEx.start_in/1`
- **THEN** the call returns `:ok` and the default system is running

#### Scenario: Failure reported

- **GIVEN** a data directory that cannot be created
- **WHEN** a caller invokes `RaftEx.start_in/1`
- **THEN** the call returns an error tuple containing the reason

### Requirement: Startup parameters

Startup SHALL apply key/value parameters to the RaftEx environment before the default system starts, so options such as the data directory, WAL sync method, and chunk size take effect.

#### Scenario: Parameter applied

- **GIVEN** a caller starting with a custom data directory parameter
- **WHEN** the default system starts
- **THEN** its configuration reports the custom directory

### Requirement: Repeated startup

Starting an already-running default system SHALL stop and replace it so the new parameters take effect, and SHALL leave the RaftEx application supervision tree healthy.

#### Scenario: Restart with new parameters

- **GIVEN** a running default system
- **WHEN** a caller starts again with a different data directory
- **THEN** the old system is stopped, a new system uses the new directory, and the call returns `:ok`

### Requirement: Persisted server configuration

When a server or cluster is started, the system SHALL durably persist the configuration needed to restart it, including server id, uid, cluster name, machine module and version, and initial members, using an atomic write.

#### Scenario: Configuration written at start

- **GIVEN** a caller starting a server
- **WHEN** startup succeeds
- **THEN** a complete configuration file exists in the server's data directory

#### Scenario: Configuration survives restart

- **GIVEN** a server that was started and then stopped
- **WHEN** the server is restarted from its data directory
- **THEN** it uses the persisted configuration rather than requiring the caller to resupply it

### Requirement: Registered-server recovery

Under the registered recovery strategy, restarting a system SHALL restart every server whose persisted configuration is present, and SHALL report a missing or invalid configuration instead of silently skipping the server.

#### Scenario: Registered server restarts

- **GIVEN** a system with a server that crashed and a persisted configuration for it
- **WHEN** recovery runs with the registered strategy
- **THEN** the server process is restarted with the same id and uid

#### Scenario: Missing configuration reported

- **GIVEN** a registered server whose configuration file is missing
- **WHEN** recovery runs
- **THEN** recovery reports the missing configuration and does not silently omit the server

### Requirement: Startup ordering

`RaftEx.start/1` SHALL ensure the `raft_ex` application is started before stopping or replacing the default system, and SHALL return `:ok` or `{:error, reason}` instead of crashing when the application was not running.

#### Scenario: Cold start

- **GIVEN** a node where `raft_ex` is not started
- **WHEN** a caller invokes `RaftEx.start/1`
- **THEN** the application starts, the default system starts, and the call returns `:ok`

#### Scenario: Repeated start

- **GIVEN** a running default system
- **WHEN** a caller invokes `RaftEx.start/1` again
- **THEN** the old system is stopped and replaced without crashing

### Requirement: Startup parameters effective

Documented startup parameters, including the WAL sync method, snapshot chunk size, pipeline depth, and replication batch size, SHALL be read from configuration and applied to the running system.

#### Scenario: WAL sync method applied

- **GIVEN** a caller starting with `wal_sync_method: :fsync`
- **WHEN** the system starts
- **THEN** WAL appends use `fsync`

#### Scenario: Chunk size applied

- **GIVEN** a caller starting with a custom snapshot chunk size
- **WHEN** a snapshot is transferred
- **THEN** chunks use the configured size

#### Scenario: Batch and pipeline limits applied

- **GIVEN** a caller starting with custom replication batch and pipeline limits
- **WHEN** the leader replicates
- **THEN** the configured limits are used
