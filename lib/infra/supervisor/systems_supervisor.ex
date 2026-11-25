defmodule RaftEx.Infra.Supervisor.SystemsSupervisor do
  @moduledoc """
  Dynamic supervisor that owns one `SystemSupervisor` per RaftEx system.
  """

  use DynamicSupervisor
  require Logger

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts), do: DynamicSupervisor.init(strategy: :one_for_one)

  def start_system(%{name: name, data_dir: dir} = config) when is_atom(name) do
    Logger.info("starting RaftEx system: #{name} in directory: #{dir}")
    RaftEx.App.System.store(config)

    spec = %{
      id: name,
      start: {RaftEx.Infra.Supervisor.SystemSupervisor, :start_link, [config]},
      type: :supervisor
    }

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, pid} ->
        :persistent_term.put(pid_key(name), pid)
        {:ok, pid}

      other ->
        other
    end
  end

  def stop_system(config) when is_map(config), do: stop_system(config.name)

  def stop_system(name) when is_atom(name) do
    children = DynamicSupervisor.which_children(__MODULE__)

    stored = :persistent_term.get(pid_key(name), nil)
    pid = find_child_pid(children, name, stored)

    result =
      case pid do
        nil -> :ok
        child_pid -> DynamicSupervisor.terminate_child(__MODULE__, child_pid)
      end

    :persistent_term.erase(pid_key(name))
    :persistent_term.erase({:"$ra_system", name})
    result
  end

  defp find_child_pid(children, name, nil) do
    case Enum.find(children, fn {id, _, _, _} -> id == name end) do
      nil -> nil
      {_, child_pid, _, _} -> child_pid
    end
  end

  defp find_child_pid(children, _name, stored) do
    if Enum.any?(children, fn {_, child_pid, _, _} -> child_pid == stored end),
      do: stored,
      else: nil
  end

  defp pid_key(name), do: {:"$ra_system_pid", name}
end
