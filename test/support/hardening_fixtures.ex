defmodule RaftEx.TestSupport.SnapshotFixture do
  @moduledoc """
  Deterministic multi-chunk snapshot fixture (task 1.4).

  Builds a state larger than one snapshot chunk without randomness so
  first/middle/last chunk paths are exercised deterministically.
  """

  @doc "Build a deterministic binary of at least `size` bytes."
  def large_state(size \\ 100_000) when is_integer(size) and size > 0 do
    chunk = "raft-snapshot-fixture-0123456789"
    repeats = div(size, byte_size(chunk)) + 1

    1..repeats
    |> Enum.map_join(fn i -> "#{chunk}#{rem(i, 10)}|" end)
    |> binary_part(0, size)
  end

  @doc "Default fixture used by placeholder tests (> 64 KiB)."
  def default, do: large_state(100_000)
end

defmodule RaftEx.TestSupport.RpcInterceptor do
  @moduledoc """
  Duplicate/reordered RPC interceptor (task 1.4).

  Pure helpers plus a recording process so tests can assert that duplicate
  and reordered deliveries are tolerated.
  """

  @doc "Duplicate a message: `[msg, msg]`."
  def duplicate(msg), do: [msg, msg]

  @doc "Swap the first two messages; shorter lists are returned unchanged."
  def reorder([a, b | rest]), do: [b, a | rest]
  def reorder(other), do: other

  @doc "Start a recorder that stores every delivered message in order."
  def start_recorder do
    {:ok, pid} = Agent.start_link(fn -> [] end)
    pid
  end

  @doc "Deliver `msg` to the recorder."
  def deliver(recorder_pid, msg) do
    Agent.update(recorder_pid, fn msgs -> msgs ++ [msg] end)
  end

  @doc "Read all recorded messages in delivery order."
  def recorded(recorder_pid) do
    Agent.get(recorder_pid, & &1)
  end
end
