# Spec Delta

## Purpose

Defines how clients submit commands to RaftEx, how concurrent submissions are batched into durable writes, how replies and pipeline notifications are routed back to the correct caller, and how submission failures are reported.

## ADDED Requirements

### Requirement: Batched durable command append

The leader SHALL batch commands that arrive while a WAL write is pending into one durable write and sync, SHALL preserve submission order within the batch, and SHALL acknowledge each caller only after its own command is durable.

#### Scenario: Concurrent commands share one sync

- **GIVEN** a leader with several commands submitted concurrently
- **WHEN** the leader appends them
- **THEN** the commands are written and synced in one batch and every caller is acknowledged after that sync

#### Scenario: Single command remains durable

- **GIVEN** a leader with one pending command and no other traffic
- **WHEN** the command is appended
- **THEN** it is written and synced before the caller is acknowledged

#### Scenario: Order preserved within batch

- **GIVEN** commands A then B submitted to the same leader
- **WHEN** both are batched into one append
- **THEN** A receives the lower log index and B the higher index

### Requirement: Command reply routing

A command reply SHALL be delivered according to its reply mode: `:leader` replies to the originating caller, `:local` replies from the server that applied the command, and `{:member, id}` replies from the named member.

#### Scenario: Leader reply

- **GIVEN** a command submitted with the default leader reply mode
- **WHEN** the command commits
- **THEN** the originating caller receives the result and the leader id

#### Scenario: Local reply

- **GIVEN** a command submitted with `reply_from: :local`
- **WHEN** the command commits
- **THEN** the reply identifies the server that applied it, not necessarily the leader

#### Scenario: Member reply

- **GIVEN** a command submitted with `reply_from: {:member, id}`
- **WHEN** the command commits
- **THEN** the reply identifies that member as the replier

### Requirement: Pipeline notifications

A pipelined command SHALL deliver exactly one notification carrying its correlation id after the command is durable, and SHALL NOT deliver a per-command reply to the submitter.

#### Scenario: Notification delivered

- **GIVEN** a pipelined command with correlation `c1`
- **WHEN** the command becomes durable
- **THEN** the submitting process receives one notification for `c1`

#### Scenario: No duplicate notification

- **GIVEN** a pipelined command whose append is retried after a transient error
- **WHEN** the command finally becomes durable
- **THEN** the submitter receives exactly one notification for that correlation id

### Requirement: Submission failure reporting

When a command cannot be durably appended, the leader SHALL return an error to every affected caller and SHALL NOT acknowledge success or advance the log as if the command were stored.

#### Scenario: WAL write failure

- **GIVEN** a leader whose WAL write fails
- **WHEN** a command is submitted
- **THEN** the caller receives an error and the command is not reported as appended

#### Scenario: Batched callers all fail

- **GIVEN** several callers batched into one append
- **WHEN** that append fails
- **THEN** every caller in the batch receives an error

### Requirement: Bounded command queue

The leader SHALL bound the number of commands waiting to be appended and SHALL reject new submissions with an explicit backpressure error when the bound is reached, rather than growing memory without limit.

#### Scenario: Queue full

- **GIVEN** a leader whose pending command queue is at its configured limit
- **WHEN** another command is submitted
- **THEN** the submission is rejected with a backpressure error and the queue does not grow
