# Spec Delta

## Purpose

Starts and configures a RaftEx system under the library's own OTP application, using its own environment and reporting startup failures instead of hiding them.

## ADDED Requirements

### Requirement: Application identity

Startup, shutdown, and configuration SHALL use the `raft_ex` OTP application and environment, and SHALL NOT stop, load, or configure an application named `ra`.

#### Scenario: Start does not touch Ra

- **GIVEN** an installed application named `ra` that is not running
- **WHEN** a caller invokes `RaftEx.start/1`
- **THEN** the `ra` application remains stopped and `raft_ex` is used for configuration

#### Scenario: Data directory from RaftEx environment

- **GIVEN** a `raft_ex` environment entry setting the data directory
- **WHEN** the default system starts
- **THEN** servers store data under that directory and not under a `ra` environment value

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
