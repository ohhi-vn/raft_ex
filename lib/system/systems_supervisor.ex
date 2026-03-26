defmodule RaftEx.SystemsSupervisor do
  use DynamicSupervisor
  require Logger

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts), do: DynamicSupervisor.init(strategy: :one_for_one)

  def start_system(%{name: name, data_dir: dir} = config) when is_atom(name) do
    Logger.info("starting RaftEx system: #{name} in directory: #{dir}")
    RaftEx.System.store(config)
    spec = %{id: name, start: {RaftEx.SystemSupervisor, :start_link, [config]}, type: :supervisor}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  def stop_system(config) when is_map(config), do: stop_system(config.name)

  def stop_system(name) when is_atom(name) do
    children = DynamicSupervisor.which_children(__MODULE__)

    case Enum.find(children, fn {id, _, _, _} -> id == name end) do
      nil -> :ok
      {_, pid, _, _} -> DynamicSupervisor.terminate_child(__MODULE__, pid)
    end
    |> tap(fn _ ->
      :persistent_term.erase({:"$ra_system", name})
    end)
  end
end
