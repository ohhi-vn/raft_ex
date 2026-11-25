defmodule RaftEx.Domain.Machine.Simple do
  @moduledoc """
  A minimal `RaftEx.Domain.Machine` implementation backed by a user function.

  The function receives each command and the current state, and returns the
  updated state together with the reply.
  """

  @behaviour RaftEx.Domain.Machine

  @impl RaftEx.Domain.Machine
  def init(%{simple_fun: fun, initial_state: initial}) do
    {:simple, fun, initial}
  end

  @impl RaftEx.Domain.Machine
  def apply(_meta, cmd, {:simple, fun, state}) do
    next = fun.(cmd, state)
    {{:simple, fun, next}, next}
  end
end
