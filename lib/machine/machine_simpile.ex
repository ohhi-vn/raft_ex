defmodule RaftEx.MachineSimple do
  @behaviour RaftEx.Machine

  @impl RaftEx.Machine
  def init(%{simple_fun: fun, initial_state: initial}) do
    {:simple, fun, initial}
  end

  @impl RaftEx.Machine
  def apply(_meta, cmd, {:simple, fun, state}) do
    next = fun.(cmd, state)
    {{:simple, fun, next}, next}
  end
end
