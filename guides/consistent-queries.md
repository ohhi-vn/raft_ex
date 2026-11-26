# Consistent Queries

Strongly consistent reads go through the leader and wait for commit.

```elixir
RaftEx.consistent_query(server_id, {:my_query, args})
```

Use `local_query/3` for stale reads from any server.