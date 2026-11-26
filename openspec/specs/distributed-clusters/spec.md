# distributed-clusters Specification

## Purpose

Enables a Raft cluster to be formed, operated, and recovered across multiple physical hosts using Erlang distribution, with deterministic server identity, reliable cross-host RPC, node discovery, partition tolerance, and host reboot recovery.

## Requirements

### Requirement: Deterministic cross-host server identity

Given a list of participating Erlang nodes, the system SHALL assign each node exactly one server id, and every host SHALL compute the same id-to-node mapping regardless of the order in which nodes are supplied.

#### Scenario: Same mapping on every host

- **WHEN** two hosts call the id builder with the same node list in different orders
- **THEN** both receive identical, sorted server-id lists

#### Scenario: One server per host

- **WHEN** a cluster is formed over N distinct nodes
- **THEN** exactly N server ids exist and each maps to a distinct node

### Requirement: Cross-host cluster formation

When every participating host invokes cluster formation with the same cluster name, machine, and node list, the system SHALL start one server per host and elect a leader reachable from all hosts.

#### Scenario: Leader elected across hosts

- **WHEN** three hosts form a cluster and wait for election
- **THEN** a single leader is elected and `find_leader/1` returns it from each host

#### Scenario: Formation without local first server

- **WHEN** a host that does not own the first server id invokes formation
- **THEN** that host starts its local servers and the owning host triggers election, and formation still succeeds

### Requirement: Cross-host RPC routing

A request addressed to a server id on a remote host SHALL be executed on that host and its reply SHALL be returned to the caller, including success and error outcomes.

#### Scenario: Remote command succeeds

- **WHEN** a caller invokes `process_command/3` targeting a server id on another host
- **THEN** the command runs on the remote host and the caller receives `{:ok, reply, leader}`

#### Scenario: Unreachable host reported

- **WHEN** a request targets a server on a host that cannot be reached
- **THEN** the call returns `{:error, :noconnection}` (or `:noproc` when the process is absent) instead of hanging

### Requirement: Cross-host join

A host not originally in the cluster SHALL be able to join via `join_cluster/5`, adding its local server as a member, and the enlarged cluster SHALL replicate subsequent writes to it.

#### Scenario: New host joins

- **WHEN** a new host calls `join_cluster/5` with a reachable seed
- **THEN** its server is added as a member and appears in `members/2` on every host

#### Scenario: Unreachable seed rejected

- **WHEN** `join_cluster/5` is given a seed host that cannot be reached
- **THEN** the call returns `{:error, :seed_unreachable}` and no local server is left half-joined

### Requirement: Node discovery and topology changes

When node discovery is enabled, the system SHALL detect newly connected and disconnected nodes and reconcile cluster membership by adding missing servers.

#### Scenario: New node discovered

- **WHEN** a new node connects while discovery is running
- **THEN** the system attempts to add its server id as a member and logs the outcome

#### Scenario: Disconnected node handled

- **WHEN** a previously connected node disconnects
- **THEN** the system records the topology change and affected server processes are notified

### Requirement: Partition tolerance

When a minority of hosts becomes unreachable, the majority partition SHALL continue to accept and commit writes, and the partitioned hosts SHALL not elect a competing leader.

#### Scenario: Majority continues under partition

- **WHEN** one of three hosts is partitioned away
- **THEN** the remaining two-host majority continues to commit commands

#### Scenario: Partitioned host does not lead

- **WHEN** a host is isolated from the majority
- **THEN** it does not become leader and rejects or redirects writes

### Requirement: Host reboot and rejoin recovery

A host restarted with the same data directory and node name SHALL rejoin the cluster and recover its term, log, and snapshot state from disk.

#### Scenario: Restarted host catches up

- **WHEN** a host reboots and reconnects with its original data directory
- **THEN** it rejoins membership and its last_applied index converges to the leader's

#### Scenario: Corrupt partial state discarded

- **WHEN** a host restarts with a partial or corrupt snapshot file
- **THEN** the corrupt file is discarded and state is recovered from the last complete snapshot or log

### Requirement: Reconnection on RPC

If a remote node connection is lost, the system SHALL attempt to reconnect on the next RPC and report failure promptly when reconnection fails.

#### Scenario: Reconnect succeeds

- **WHEN** a remote node temporarily drops and returns
- **THEN** the next RPC succeeds after reconnection

#### Scenario: Reconnect fails fast

- **WHEN** a remote node remains unreachable
- **THEN** the RPC returns an error within the configured timeout rather than hanging

### Requirement: Consistent membership view across hosts

`members/2` and `members_info/2` SHALL return the same member set and roles when invoked on any host in the cluster.

#### Scenario: Uniform membership

- **WHEN** `members/2` is called on two different hosts
- **THEN** both return identical member lists

### Requirement: Host-aware timeouts and diagnostics

Distributed operations SHALL use host-aware (longer) default timeouts and SHALL produce diagnostics identifying the failing host or node.

#### Scenario: Timeout scaled for hosts

- **WHEN** a distributed operation runs over multiple hosts
- **THEN** default timeouts accommodate cross-host latency and are configurable via options

#### Scenario: Failure identifies host

- **WHEN** a distributed operation fails on a remote host
- **THEN** the error or log identifies the host or node involved