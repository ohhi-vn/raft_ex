# Membership Changes

RaftEx supports single-server membership changes (add/remove/promote/demote).

## Roles

- `:voter` — participates in quorum.
- `:promotable` — can be promoted to voter.
- `:non_voter` — receives log but not counted in quorum.

## Operations

```elixir
RaftEx.add_member(server_ref, new_server_id, :voter)
RaftEx.remove_member(server_ref, server_id)
RaftEx.promote_member(server_ref, server_id)
RaftEx.demote_member(server_ref, server_id)
```