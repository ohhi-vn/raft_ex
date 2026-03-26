defmodule RaftEx.LogWalSupervisor do
  @moduledoc """
  Supervisor for the WAL (Write-Ahead Log) and its helper processes.

  Restarts the WAL on failure and manages the WAL sync worker.
  """

  use Supervisor

  def start_link(config) do
    {:ok, sup_name} = RaftEx.System.lookup_name(config.system, :wal_sup)
    Supervisor.start_link(__MODULE__, config, name: sup_name)
  end

  @impl Supervisor
  def init(config) do
    children = [
      %{
        id:      :ra_log_wal,
        start:   {RaftEx.LogWal, :start_link, [config]},
        restart: :permanent,
        shutdown: 30_000
      }
    ]

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 3, max_seconds: 5)
  end
end
