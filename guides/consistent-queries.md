# Consistent Queries

Strongly consistent reads go through the leader and wait for commit.

```elixir
{:ok, result, leader} = RaftEx.consistent_query(server_id, fn state -> state end)
```

The leader collects heartbeat acknowledgements from a majority before
executing the query, ensuring the read reflects all committed writes.

Use `local_query/3` for stale reads from any server (lower latency, no
quorum round-trip):

```elixir
{:ok, result} = RaftEx.local_query(server_id, fn state -> state end)
```

## When to use which

- `consistent_query/3` — when you need linearizable reads.
- `local_query/3` — for dashboards, metrics, or stale-tolerant UIs.
