defmodule RaftEx.Supervisor do
  use Supervisor

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
    RaftEx.Env.configure_logger(logger)

    children = [
      RaftEx.MachineEts,
      RaftEx.MetricsEts,
      RaftEx.NodeMonitor,
      {RaftEx.SystemsSupervisor, []}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 1, max_seconds: 5)
  end
end
