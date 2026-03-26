
defmodule RaftEx.LogPreInit do
  @moduledoc """
  Temporary barrier process that allows the log supervisor's children to
  signal when they are ready for writes.

  This avoids race conditions between the WAL startup and the first server
  process trying to append entries.
  """

  use GenServer

  def start_link(system) when is_atom(system) do
    GenServer.start_link(__MODULE__, system, [])
  end

  @impl GenServer
  def init(_system), do: {:ok, %{}, {:continue, :ready}}

  @impl GenServer
  def handle_continue(:ready, state), do: {:noreply, state}
end
