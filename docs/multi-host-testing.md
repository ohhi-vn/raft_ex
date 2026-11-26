# Multi-host testing

RaftEx tests normally run in a single BEAM. Multi-host scenarios verify
behaviour across separate machines (node discovery, distribution, partitions,
host reboot).

## Prerequisites

- Erlang/OTP and Elixir installed on every host, same versions.
- A shared cookie (`--cookie` / `RAFT_COOKIE`).
- epmd running on each host (port 4369 by default).
- Open ports for distribution (default range 9100-9200).
- Reachable hostnames (DNS or /etc/hosts).
- Shared code: either a common NFS mount or rsync the `_build` directory.

## Starting a node

On each host:

```bash
scripts/start_raft_node.sh raft@host1 secret /var/lib/raft_ex 4369
```

Verify connectivity from the coordinator:

```elixir
Node.connect(:"raft@host1")
```

## Running the suite

```bash
RAFT_EX_HOSTS=host1,host2 RAFT_COOKIE=secret mix test --include multi_host
```

Without `RAFT_EX_HOSTS`, multi-host tests are excluded (default).

## Preflight checks

- `epmd -names` lists the node on each host.
- `ping host1` / `nc -z host1 4369` succeed.
- Cookie matches exactly (case-sensitive).
- `erl -name probe@host1 -setcookie secret -eval 'io:format("~p~n",[node()]),halt().'` works.
- Firewall allows the distribution port range.

## Partition simulation

- `:erlang.disconnect_node/1` from the coordinator, or
- Firewall drop rules between hosts.

## Host reboot / rejoin

Stop the node on one host, wait, restart with the same data dir and cookie,
then verify it rejoins and catches up from a snapshot.

## Security

The shared cookie grants full node access. Never use a production cookie in
tests, and never expose the distribution ports publicly.
