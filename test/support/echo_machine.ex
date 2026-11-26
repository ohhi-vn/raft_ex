defmodule RaftEx.TestSupport.EchoMachine do
  @moduledoc "A trivial state machine that echoes commands; serializable across nodes."

  @behaviour RaftEx.Domain.Machine

  def init(_conf), do: 0

  def apply(_meta, cmd, state) do
    {state, cmd}
  end

  def tick(state), do: state

  def version(state), do: state

  def aux(state), do: state
end
