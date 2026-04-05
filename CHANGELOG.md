# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2] - Current

### Critical Fixes
- **Vote Attribution Bug**: Fixed election vote counting - votes were incorrectly attributed to self instead of replying peer, making multi-node elections impossible to win
- **Follower AppendEntries Stub**: Replaced stub `handle_follower` with proper `RpcHandler` delegation for term checking, log consistency verification, entry appending, and commit index advancement
- **Log Truncation No-Op**: Implemented actual `truncate_log_from` to remove conflicting entries when leader sends entries with different terms at same index (Raft Log Matching Property)
- **Vote Term Ordering**: Fixed election livelock risk - now checks log up-to-date BEFORE updating term/voted_for when candidate has higher term
- **Write-Ahead Ordering**: Fixed `Log.write/2` to write to WAL before updating in-memory state, ensuring true write-ahead semantics
- **Segment Durability**: Added `:file.sync/1` calls after segment append and seal operations for crash safety

### Important Fixes
- **ETS Table Leak**: Fixed `LogMeta` to delete ETS table on terminate, preventing resource leaks on restart
- **Election Reply Tracking**: Added peer ID tracking in process dictionary for correlating election RPC replies with sender
- **Snapshot Transfer**: Implemented leader-side snapshot chunk sending with peer status tracking and completion handling

### New Features
- **Network Layer**: Added `RaftEx.Network` module for unified local/remote RPC routing with node monitoring
- **Segment Files**: Complete immutable segment file implementation with fsync, recovery, truncation, and deletion
- **Server Process States**: Full implementation of `candidate` and `pre_vote` states with election timeout handling
- **Test Coverage**: Added 11 new tests (96 → 107) covering segment writer, network layer, and cluster operations

### Code Quality
- Added `excoveralls`, `dialyxir`, `credo` for test coverage and static analysis
- Added `quality` and `quality.ci` aliases for running format, credo, and dialyzer

## [0.0.1] - Initial Release

### Features
- Initial experimental Elixir port of RabbitMQ RA Raft consensus algorithm
- Leader election with pre-vote support
- Write-ahead log (WAL) with batched writes
- Log replication and snapshotting
- Cluster membership management
- Pluggable state machine interface
- Effects system for reply routing and notifications
- 85 passing unit tests