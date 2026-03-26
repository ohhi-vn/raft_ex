defmodule RaftEx.ServerSupervisor do
  use Supervisor

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config)
  end

  @impl Supervisor
  def init(%{id: id} = config) do
    name = RaftEx.Lib.ra_server_id_to_local_name(id)

    children = [
      %{
        id: name,
        type: :worker,
        restart: :transient,
        start: {RaftEx.ServerProc, :start_link, [config]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 2, max_seconds: 5)
  end
end
