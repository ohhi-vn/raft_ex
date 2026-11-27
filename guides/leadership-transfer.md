# Leadership Transfer

Transfer leadership to a caught-up voter:

```elixir
{:ok, target} = RaftEx.transfer_leadership(leader_id, target_id)
```

The target must be a voter and caught up to the leader's commit index.
If not, the call returns `{:error, :not_caught_up}` (this covers both
non-voters and lagging voters); transferring to self returns
`{:error, :self}`, and an unknown target returns `{:error, :not_member}`.

Use this for graceful failover, rolling restarts, or load shedding.
