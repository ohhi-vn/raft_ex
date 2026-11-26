# Getting Started

This guide walks you through installing RaftEx, starting a system, forming a cluster,
and sending a command.

## Installation

Add `:raft_ex` to your `mix.exs` dependencies:

```elixir
{:raft_ex, "~> 0.5"}
```

Then run:

```sh
mix deps.get
```

## Starting a system

```elixir
RaftEx.start()
```

Or with a custom data directory:

```elixir
RaftEx.start_in("/var/lib/raft_ex")
```

## Forming a cluster

```elixir
machine = RaftEx.TestSupport.EchoMachine  # or your own machine module
server_ids = [{:s1, node()}, {:s2, node()}, {:s3, node()}]

{:ok, started, []} =
  RaftEx.start_cluster(:default, :my_cluster, machine, server_ids)
```

## Sending a command

```elixir
{:ok, reply, leader} = RaftEx.process_command({:s1, node()}, {:put, :k, :v})
```

## Querying state

```elixir
RaftEx.local_query({:s1, node()}, fn state -> state end)
```

## Next steps

- See `guides/architecture.md` for the internal design.
- See `guides/persistence.md` for WAL and recovery.
- See `RaftEx` module docs for the full API.
