defmodule RaftEx.TestSupport.LargeStateMachine do
  @moduledoc """
  A state machine that produces a controllably large state, used to force
  multi-chunk snapshot transfers in distributed tests.
  """

  @behaviour RaftEx.Domain.Machine

  def init(_conf), do: %{counter: 0, blob: <<>>}

  def apply(_meta, {:increment, n}, state) do
    new_state = %{state | counter: state.counter + n, blob: state.blob <> :binary.copy(<<0>>, n)}
    {new_state, new_state.counter}
  end

  def apply(_meta, {:get}, state), do: {state, state.counter}

  def apply(_meta, {:release_cursor, idx}, state), do: {state, {:release, idx}}

  def tick(state), do: state

  def version(state), do: state.counter

  def aux(state), do: state
end
