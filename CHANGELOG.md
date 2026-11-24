# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - Current

### Added
- **Per-Server WAL Isolation**: Each server (identified by its `:uid`) now owns its own WAL file at `<wal_data_dir>/<uid>/wal.dat`, with an independent buffer, index range, file handle, and pending-write set. A single WAL process can serve every server in a system without interleaving their entries. The WAL APIs now take a `uid`: `append/4`, `append_async/3`, `truncate/4`, `info/2`, and `read/4`; `overview/1` reports aggregate state across all files.
- **Deterministic Cluster UIDs**: `RaftEx.new_uid/1` now derives a stable UID from a SHA-256 digest of the source, so restarting or re-forming a cluster with the same name recovers its existing log instead of starting from an empty directory.
- **Leaderboard.forget_leader/1**: Added a helper that clears a stopped server's cached leader entry while retaining the cluster's member list, so `find_leader/1` can still query the remaining members.

### Changed
- **WAL Record Format v2**: WAL records now include the entries-blob byte size in the header so each record can be read back exactly. Legacy v1 records (count-only header) can no longer be decoded reliably; recovery now stops cleanly instead of returning a partial log.
- **Synchronous `last_applied` Checkpointing**: `RaftEx.Domain.Server.persist_last_applied/1` now uses `LogMeta.store_sync/4` and is checkpointed immediately when committed entries are applied. This prevents a client-visible state change from being lost if the server is stopped in the same tick.
- **Cross-Node RPC Routing**: Remote `safe_call/3` now uses `:erpc` to invoke a local entry point on the target node, with structured error mapping (`:noproc`, `:nodedown`, `:shutdown`, timeouts). `pid_to_server_id/1` resolves registered names on remote nodes via `:erpc`. Remote sends now wrap messages with the sender pid so the receiver can derive `rpc_from`, matching the local send path.
- **WAL-Backed Leader Appends**: Leader command and cluster-change entries now go through `Log.write/2` (write-ahead) instead of in-memory `Log.append/2`, with graceful `{:not_appended, reason, ...}` handling when the WAL is unavailable.

### Fixed
- **Server Stop/Delete Reliability**: `stop_server/2` and `delete_server/2` now locate the server's supervisor child by walking the `DynamicSupervisor` tree and delegate termination to `stop_server_rpc/2`, which also forgets the cached leader. Deleting an already-stopped server is now idempotent instead of raising `FunctionClauseError`.
- **Recovery Fold Strategy**: `RaftEx.Infra.Persistence.Log.fold/6` now handles the `:return` strategy used when scanning entries above the commit index during recovery.

## [0.3.0] - 2026-11-24

### Added
- **Commit Index Propagation via Heartbeats**: Added a `:commit_index` field to `RaftEx.Domain.Types.HeartbeatRpc`. Leader heartbeats now carry the current commit index; followers advance their own commit index from it and apply newly committed entries. Leaders rebroadcast commit advances to their peers, and followers handle `HeartbeatRpc` explicitly via `RaftEx.Domain.Server.handle_follower/2`.
- Exposed `RaftEx.Domain.Server.apply_committed/2` for applying committed log entries to the state machine.

### Changed
- **Follower Reply Ownership**: Followers applying committed entries no longer emit client replies. Only the leader owns the caller's reply alias, preventing duplicate responses when the same entry is applied across the cluster.
- **System Lifecycle Tracking**: `RaftEx.Infra.Supervisor.SystemsSupervisor` now records started system PIDs in `:persistent_term` and uses them in `stop_system/1` to reliably terminate the correct child, falling back to child-ID lookup when no stored PID is available.

### Fixed
- **WAL Directory Config**: `RaftEx.Infra.Persistence.LogWal` now reads the `:dir` key (as built by `LogWalSupervisor` from `:wal_data_dir`), falling back to `:data_dir` for callers passing the full system config directly.
- **RPC Sender Resolution**: `RaftEx.App.ServerRuntime.pid_to_server_id/1` now handles both the tuple and list forms returned by `Process.info(pid, :registered_name)` and guards against non-atom names.
- **Metrics Safety**: `RaftEx.key_metrics/1` now normalizes non-map counter values (e.g. `:undefined` for unregistered servers) to an empty map instead of raising `BadMapError`.

## [0.2.0] - 2026-11-24

### Added
- **Clean Architecture Layout**: Modules reorganized into four layers — `RaftEx.Domain.*` (pure Raft logic), `RaftEx.App.*` (OTP orchestration), `RaftEx.Infra.*` (adapters), and the top-level `RaftEx` public facade. The Raft core can now be exercised without starting OTP.
- **Plug-style Middleware Pipelines**: Added `RaftEx.Domain.Conn` (Plug-compatible context with `assign/3`, `halt/1`, `put_resp/2`, `get_assign/3`), the `RaftEx.Domain.Plug` behaviour (`init/1`, `call/2`), and the `RaftEx.Domain.Pipeline` runner with halt semantics. Pipelines are pure and add no runtime dependency.
- **Configurable Command & RPC Pipelines**: Added `:command_plugs` and `:rpc_plugs` keys to system and server config. Plugs are initialized once at server startup and run around command handling and incoming AppendEntries/vote/snapshot RPCs; a halted pipeline short-circuits processing and replies from the conn. Default is an empty pipeline, so existing behavior is unchanged.
- **Shared Test Support**: Added `RaftEx.TestSupport` with a temporary-directory helper and added `test/support` to `elixirc_paths(:test)`.

### Changed
- **Module Namespace Migration (breaking)**: Public/internal modules moved to layered namespaces, e.g. `RaftEx.Server` → `RaftEx.Domain.Server`, `RaftEx.ServerProc` → `RaftEx.App.ServerRuntime`, `RaftEx.Log` → `RaftEx.Infra.Persistence.Log`, `RaftEx.Network` → `RaftEx.Infra.Network.Transport`, `RaftEx.Types` → `RaftEx.Domain.Types`, `RaftEx.Machine` → `RaftEx.Domain.Machine`. README examples and the Mix task were updated accordingly.
- **OTP Application Callback**: Renamed `RaftEx.Application` to `RaftEx.OTPApplication` and updated `mix.exs`.
- **Naming Fixes**: Corrected the `MachineSimple` filename typo (`simpile` → `simple`, now `RaftEx.Domain.Machine.Simple`) and renamed `RaftEx.Monitors` to the singular `RaftEx.Infra.Observability.Monitor`.
- **Test Suite Reorganization**: Tests are now split by layer into `test/domain/`, `test/app/`, and `test/infra/`, with integration/facade tests under `test/integration/` and new coverage for previously untested modules (Election, RpcHandler, network, persistence, supervisors, Mix task). Test count grew from 130 to 552 and overall line coverage from 37.7% to 82.4%.

### Fixed
- **`Seq.in_range/2` Argument Order**: Fixed reversed arguments passed through `floor/2` and `limit/2` that raised `FunctionClauseError`; now computes `limit(ending, floor(start, seq))`.
- **`Cluster.update_peer/3` Call Order**: Fixed all four `RpcHandler` call sites that passed `cluster` first instead of `(peer_id, update, cluster)`, which raised `FunctionClauseError`.

### Removed
- **`RaftEx.Lib`**: The catch-all module was removed; its helpers were consolidated into `RaftEx.Domain.Utils` (type coercions, UID generation, server-ID helpers, and miscellaneous utilities).

## [0.0.2]

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
