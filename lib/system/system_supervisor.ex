defmodule RaftEx.SystemSupervisor do
  use Supervisor
  require Logger

  def start_link(config), do: Supervisor.start_link(__MODULE__, config)

  @impl Supervisor
  def init(%{data_dir: data_dir, name: name} = cfg) do
    case File.mkdir_p(data_dir) do
      :ok ->
        Logger.debug("RaftEx system '#{name}' starting")

        children = [
          {RaftEx.LogEts, cfg},
          {RaftEx.LogSupervisor, cfg},
          {RaftEx.ServerSupSupervisor, cfg},
          {RaftEx.SystemRecover, name}
        ]

        Supervisor.init(children, strategy: :one_for_all, max_restarts: 1, max_seconds: 5)

      {:error, code} ->
        Logger.error("Failed to create RaftEx data directory at '#{data_dir}': #{code}")
        raise "RaftEx could not create its data directory"
    end
  end
end
