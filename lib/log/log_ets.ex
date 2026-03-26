defmodule RaftEx.LogEts do
  @moduledoc """
  Initialises the ETS tables used by the WAL and segment reader layer.

  Started as the first child of `RaftEx.LogSupervisor` so that tables are
  available before the WAL and segment writer processes start.
  """

  use GenServer

  @open_mem_tbls_flags  [:named_table, :public, {:write_concurrency, true}]
  @log_ets_flags        [:named_table, :public, {:read_concurrency, true}]

  def start_link(%{names: names} = cfg) do
    GenServer.start_link(__MODULE__, cfg, name: Map.fetch!(names, :log_ets))
  end

  @impl GenServer
  def init(%{names: %{open_mem_tbls: open_mem_tbls, log_ets: log_ets}}) do
    :ets.new(open_mem_tbls, [:set | @open_mem_tbls_flags])
    :ets.new(log_ets,       [:set | @log_ets_flags])
    {:ok, %{}}
  end
end
