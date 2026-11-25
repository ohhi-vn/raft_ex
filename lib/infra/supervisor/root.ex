defmodule RaftEx.Infra.Supervisor.Root do
  @moduledoc """
  Top-level supervisor that initialises shared ETS tables and starts the
  application's long-lived children.
  """

  use Supervisor

  alias RaftEx.Infra.Config.Env

  @tables [:ra_state, :ra_open_file_metrics, :ra_io_metrics]

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    for table <- @tables do
      :ets.new(table, [:named_table, :public, {:write_concurrency, true}])
    end

    logger = Application.get_env(:ra, :logger_module, Logger)
    Env.configure_logger(logger)

    children = [
      RaftEx.Infra.Process.MachineEts,
      RaftEx.Infra.Observability.MetricsEts,
      RaftEx.Infra.Network.NodeMonitor,
      {RaftEx.Infra.Supervisor.SystemsSupervisor, []}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 1, max_seconds: 5)
  end
end
