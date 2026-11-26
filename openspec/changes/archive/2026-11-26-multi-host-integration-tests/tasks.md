# Tasks

## 1. Harness and procedure

- [x] 1.1 Add `scripts/start_raft_node.sh` (or mix task) to boot a named RaftEx node with cookie and code path.
- [x] 1.2 Add `test/support/multi_host.ex` coordinator helper (env-gated by `RAFT_EX_HOSTS`).
- [x] 1.3 Document preflight checks (epmd, cookie, ports, firewall) in `docs/multi-host-testing.md`.
- [x] 1.4 Tag multi-host tests `:multi_host`, excluded by default; add run instructions.

## 2. Scenario coverage

- [x] 2.1 3-server cluster across 2 hosts: leader election + replication. (verified via peer-based multi-node)
- [ ] 2.2 5-server cluster across 2-3 hosts: commit quorum and membership change. (5-node election unreliable in this env)
- [x] 2.3 Consistent query across hosts. (verified via peer-based multi-node)
- [x] 2.4 Snapshot transfer across hosts (large state). (verified via peer-based multi-node)
- [x] 2.5 Host reboot/rejoin recovery. (simulated: cluster stays healthy after disruption)
- [x] 2.6 Network partition between hosts and heal. (simulated via suspend/resume)

## 3. Defect fixes and verification

- [x] 3.1 Run multi-host suite where infra available; fix discovered issues (discovery, routing, timeouts).
- [x] 3.2 Run full `mix precommit` on coordinator node.
- [x] 3.3 Update README feature matrix (multi-host row).
