# Distributed testing

RaftEx ships an opt-in distributed test suite tagged `:distributed`. It spawns
separate BEAM nodes via `:peer` and runs consensus/snapshot scenarios across
them.

## Running

Distributed tests require a named (distributed) node:

```bash
elixir --sname raft_test_node -S mix test --include distributed
```

By default (`mix test`) they are excluded, so CI and local runs stay fast and
deterministic.

## Multi-host

For multi-host runs, set `RAFT_EX_HOSTS=host1,host2` and use the
`RaftEx.TestSupport.DistributedCluster` helper with `:host`. Multi-host runs
are manual and require a shared cookie, reachable epmd, and open ports.

## Reproducibility

- Seeds: ExUnit prints the seed per run; pass `--seed N` to reproduce.
- Timeouts: default 60s per test; override with `--timeout`.
- Flaky timing: distributed tests use bounded waits (poll loops with 250ms
  steps, up to ~10s). A failure that disappears on retry is usually timing;
  re-run with `--seed` to confirm.

## Fault injection

Set `RAFT_EX_FAULTS=1` to enable env-gated network-delay/partition scenarios
(see `test/support/fault_injection.ex`). These are disabled by default.
