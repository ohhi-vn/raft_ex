defmodule RaftEx.Infra.Supervisor.Root do
  @moduledoc """
  Top-level supervisor that initialises shared ETS tables and starts the
  application's long-lived children.
  """

  use Supervisor

  @tables [:ra_state, :ra_open_file_metrics, :ra_io_metrics, :ra_pid_to_server_id]

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    for table <- @tables do
      :ets.new(table, [:named_table, :public, {:write_concurrency, true}])
    end

    children = [
      RaftEx.Infra.Process.MachineEts,
      RaftEx.Infra.Observability.MetricsEts,
      RaftEx.Infra.Network.NodeMonitor,
      {RaftEx.Infra.Supervisor.SystemsSupervisor, []}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 1, max_seconds: 5)
  end
end
