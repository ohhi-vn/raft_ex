defmodule RaftEx.MachineEts do
  use GenServer

  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def create_table(name, opts), do: GenServer.call(__MODULE__, {:new_ets, name, opts})

  @impl GenServer
  def init([]), do: {:ok, %{}}

  @impl GenServer
  def handle_call({:new_ets, name, opts}, _from, state) do
    unless Map.has_key?(state, name), do: :ets.new(name, opts)
    {:reply, :ok, Map.put(state, name, :ok)}
  end

  @impl GenServer
  def handle_cast({:new_ets, name, opts}, state) do
    unless Map.has_key?(state, name), do: :ets.new(name, opts)
    {:noreply, Map.put(state, name, :ok)}
  end
end
