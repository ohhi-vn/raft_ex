# Spec Delta

## MODIFIED Requirements

### Requirement: Partition tolerance

When a minority of hosts becomes unreachable, the majority partition SHALL continue to accept and commit writes, the partitioned hosts SHALL not elect a competing leader, and sends to an unreachable host SHALL NOT stall replication to reachable hosts.

#### Scenario: Majority continues under partition

- **WHEN** one of three hosts is partitioned away
- **THEN** the remaining two-host majority continues to commit commands

#### Scenario: Partitioned host does not lead

- **WHEN** a host is isolated from the majority
- **THEN** it does not become leader and rejects or redirects writes

#### Scenario: Unreachable peer does not stall the majority

- **GIVEN** a leader whose connection to one follower is down
- **WHEN** the leader replicates to the remaining followers
- **THEN** those followers continue to receive batches while the unreachable peer is retried separately

### Requirement: Reconnection on RPC

If a remote node connection is lost, the system SHALL attempt to reconnect on the next RPC, SHALL report failure promptly when reconnection fails, and SHALL NOT sleep or block the consensus process while reconnecting.

#### Scenario: Reconnect succeeds

- **WHEN** a remote node temporarily drops and returns
- **THEN** the next RPC succeeds after reconnection

#### Scenario: Reconnect fails fast

- **WHEN** a remote node remains unreachable
- **THEN** the RPC returns an error within the configured timeout rather than hanging

#### Scenario: Reconnect does not block consensus

- **GIVEN** a leader with an unreachable follower
- **WHEN** a send to that follower fails
- **THEN** the leader schedules a retry and continues processing commands and other peers without sleeping

## ADDED Requirements

### Requirement: Sender identity in RPC envelope

Every cross-host RPC SHALL carry the sender's server id, and the receiver SHALL resolve the sender locally without a remote process lookup.

#### Scenario: Remote AppendEntries identifies sender

- **GIVEN** a follower receiving AppendEntries from a remote leader
- **WHEN** it processes the message
- **THEN** it determines the leader id from the message envelope without a remote `:erpc` call

#### Scenario: Unknown sender rejected

- **GIVEN** an RPC whose envelope carries no sender id and whose payload has no leader id
- **WHEN** the receiver processes it
- **THEN** it rejects the message with a protocol error instead of guessing the sender

### Requirement: Seed node honored

Cluster formation and join SHALL use the configured seed node when one is supplied, and SHALL reject a seed node that is not part of the supplied node set.

#### Scenario: Configured seed used

- **GIVEN** a formation request with an explicit seed node
- **WHEN** the cluster forms
- **THEN** that node is used to trigger the election

#### Scenario: Seed outside node set rejected

- **GIVEN** a formation request whose seed node is not in the node list
- **WHEN** the request runs
- **THEN** it returns an error instead of silently choosing another node

### Requirement: Join success requires membership

`join_cluster/5` SHALL return success only after the local server appears in the cluster membership, SHALL propagate add-member errors, and SHALL return `{:error, :seed_unreachable}` for an unreachable seed without leaving a half-joined server.

#### Scenario: Successful join confirmed

- **GIVEN** a reachable seed
- **WHEN** a new host joins
- **THEN** the call returns success only after the local server is a member

#### Scenario: Add-member failure propagated

- **GIVEN** a reachable seed that rejects the add-member request
- **WHEN** the join runs
- **THEN** the call returns an error and reports the local server as not joined

#### Scenario: Unreachable seed rejected

- **GIVEN** a seed host that cannot be reached
- **WHEN** the join runs
- **THEN** the call returns `{:error, :seed_unreachable}` and no local server remains half-joined

### Requirement: Network events handled

The system SHALL handle network-up and network-down events by updating peer status and triggering reconciliation or catch-up, instead of ignoring the event.

#### Scenario: Node down updates status

- **GIVEN** a connected peer node
- **WHEN** the node disconnects
- **THEN** the peer is marked unreachable and replication retries are scheduled

#### Scenario: Node up triggers catch-up

- **GIVEN** a peer that was unreachable
- **WHEN** it reconnects
- **THEN** the leader schedules replication so the peer catches up
