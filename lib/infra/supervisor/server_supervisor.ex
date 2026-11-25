defmodule RaftEx.Infra.Supervisor.ServerSupervisor do
  @moduledoc """
  Supervisor for a single Raft server process and its runtime children.
  """

  use Supervisor

  alias RaftEx.Domain.Utils

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config)
  end

  @impl Supervisor
  def init(%{id: id} = config) do
    name = Utils.ra_server_id_to_local_name(id)

    children = [
      %{
        id: name,
        type: :worker,
        restart: :transient,
        start: {RaftEx.App.ServerRuntime, :start_link, [config]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 2, max_seconds: 5)
  end
end
