# Spec Delta

## MODIFIED Requirements

### Requirement: RPC path pipeline

The system SHALL run the configured RPC pipeline around every incoming RPC type, including AppendEntries, RequestVote, PreVote, Heartbeat, InstallSnapshot, and TimeoutNow, before protocol handling.

#### Scenario: Plugs execute on RPC

- **WHEN** an RPC is received by a server
- **THEN** the RPC pipeline plugs run before protocol handling

#### Scenario: Plugs execute on AppendEntries

- **WHEN** an AppendEntries RPC is received by a server
- **THEN** the RPC pipeline plugs run before the log is modified

#### Scenario: Plugs execute on vote RPCs

- **WHEN** a RequestVote or PreVote RPC is received
- **THEN** the RPC pipeline plugs run before the vote is evaluated

#### Scenario: Plugs execute on snapshot and heartbeat RPCs

- **WHEN** an InstallSnapshot or Heartbeat RPC is received
- **THEN** the RPC pipeline plugs run before the RPC is handled

#### Scenario: Halted RPC

- **WHEN** an RPC pipeline plug halts
- **THEN** the RPC is not processed and an appropriate reply is sent
