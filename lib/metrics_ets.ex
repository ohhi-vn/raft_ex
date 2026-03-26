defmodule RaftEx.MetricsEts do
  use GenServer

  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @impl GenServer
  def init([]) do
    flags = [:named_table, {:read_concurrency, true}, {:write_concurrency, true}, :public]
    :ets.new(:ra_log_metrics, [:set | flags])
    RaftEx.Counters.init()
    RaftEx.Leaderboard.init()
    :ets.new(:ra_log_snapshot_state, [:set | flags])
    {:ok, %{}}
  end

  @impl GenServer
  def handle_call(_, _from, state), do: {:reply, :ok, state}

  @impl GenServer
  def handle_cast(_, state), do: {:noreply, state}
end
