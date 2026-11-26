# Leadership Transfer

Transfer leadership to a caught-up voter:

```elixir
RaftEx.transfer_leadership(leader_id, target_id)
```

The target must be a voter and caught up to the leader's commit index.