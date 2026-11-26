# Jepsen tests for RaftEx

This directory contains a Jepsen test harness for RaftEx. Jepsen requires
Clojure tooling (Leiningen) and a set of test nodes reachable via SSH.

## Prerequisites

- Leiningen (`lein`) installed.
- A set of Linux test nodes (VMs or containers) with SSH access.
- Elixir/Erlang and a RaftEx release installed on each node at `/opt/raft_ex`.

## Running

```bash
cd jepsen
lein run test --nodes n1,n2,n3 --time-limit 60
```

Notes:
- Jepsen connects to nodes over SSH by default. Use `--username` / `--password`
  (or `--ssh-private-key`) to authenticate.
- Use `--no-ssh` together with a single `--node` for a local dry run (the
  DB must already be installed on that node).

## Notes

- The client currently returns placeholder values; wire it to RaftEx's actual
  API (consistent queries / process commands) over an RPC transport (e.g. HTTP
  or Erlang distribution) for real results.
- The nemesis uses random half-partitions; extend with crashes, pauses, etc.
- Linearizability is checked via Knossos.

## Status

Experimental. Not run in CI. See the in-process linearizability checker in
`test/support/linearizability.ex` for the lightweight alternative.