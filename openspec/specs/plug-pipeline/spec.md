# plug-pipeline Specification

## Purpose

Lets users compose cross-cutting behavior around RaftEx command and RPC processing using familiar Plug-style middleware pipelines without modifying core consensus logic.

## Requirements

### Requirement: Plug behaviour

The system SHALL define a plug behaviour compatible with Plug conventions, requiring `init/1` and `call/2` callbacks operating on a RaftEx connection context.

#### Scenario: Module plug

- **WHEN** a module plug is registered in a pipeline
- **THEN** the system calls its `init/1` at configuration time and `call/2` at runtime with the connection context

#### Scenario: Function plug

- **WHEN** a function plug is registered in a pipeline
- **THEN** the system invokes it at runtime with the connection context and options

### Requirement: Pipeline runner

The system SHALL provide a pipeline runner that executes a list of plugs in order, passing the connection context through each plug.

#### Scenario: Sequential execution

- **WHEN** a pipeline contains multiple plugs
- **THEN** each plug receives the context returned by the previous plug

#### Scenario: Halt

- **WHEN** a plug calls halt on the context
- **THEN** the runner stops executing remaining plugs and returns the halted context

### Requirement: Command path pipeline

The system SHALL run the configured command pipeline around command handling in the leader path.

#### Scenario: Plugs execute on command

- **WHEN** a command is processed by the leader
- **THEN** the command pipeline plugs run before state-machine apply

#### Scenario: Halted command

- **WHEN** a command pipeline plug halts
- **THEN** the command is not applied and the halted response is returned to the caller

### Requirement: RPC path pipeline

The system SHALL run the configured RPC pipeline around incoming RPC handling.

#### Scenario: Plugs execute on RPC

- **WHEN** an RPC is received by a server
- **THEN** the RPC pipeline plugs run before protocol handling

#### Scenario: Halted RPC

- **WHEN** an RPC pipeline plug halts
- **THEN** the RPC is not processed and an appropriate reply is sent

### Requirement: Pipeline configuration

The system SHALL allow command and RPC pipelines to be configured per system and per server via config keys `command_plugs` and `rpc_plugs`.

#### Scenario: Default empty pipeline

- **WHEN** no plugs are configured
- **THEN** processing behaves exactly as without pipelines

#### Scenario: Configured plugs

- **WHEN** plugs are configured
- **THEN** they are initialized at startup and used for all subsequent processing

### Requirement: Connection context

The system SHALL provide a connection context struct carrying request data, response data, assigns, and halted state, compatible with Plug.Conn semantics where practical.

#### Scenario: Context creation

- **WHEN** processing begins
- **THEN** a connection context is created with the incoming request populated

#### Scenario: Assigns

- **WHEN** a plug assigns a value
- **THEN** subsequent plugs can read that value from the context
