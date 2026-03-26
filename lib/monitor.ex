defmodule RaftEx.Monitors do
  @type component :: :machine | :aux | :snapshot_sender
  @type t :: map()

  def init, do: %{}

  def add(pid, component, monitors) when is_pid(pid) do
    case monitors do
      %{^pid => {mref, components}} ->
        Map.put(monitors, pid, {mref, Map.put(components, component, :ok)})

      _ ->
        mref = Process.monitor(pid)
        Map.put(monitors, pid, {mref, %{component => :ok}})
    end
  end

  def add(node, component, monitors) when is_atom(node) do
    monitors =
      case monitors do
        %{^node => components} ->
          unless Map.has_key?(components, component), do: emit_current_node_state(node)
          Map.put(monitors, node, Map.put(components, component, :ok))

        _ ->
          emit_current_node_state(node)
          Map.put(monitors, node, %{component => :ok})
      end

    monitors
  end

  def remove(target, component, monitors) do
    case monitors do
      %{^target => {mref, components}} ->
        new_components = Map.delete(components, component)

        if map_size(new_components) == 0 do
          Process.demonitor(mref)
          Map.delete(monitors, target)
        else
          Map.put(monitors, target, {mref, new_components})
        end

      %{^target => components} when is_map(components) ->
        new_components = Map.delete(components, component)

        if map_size(new_components) == 0 do
          Map.delete(monitors, target)
        else
          Map.put(monitors, target, new_components)
        end

      _ ->
        monitors
    end
  end

  def remove_all(component, monitors) do
    Enum.reduce(Map.keys(monitors), monitors, fn t, acc ->
      remove(t, component, acc)
    end)
  end

  def handle_down(target, monitors) when is_pid(target) or is_atom(target) do
    case Map.pop(monitors, target) do
      {{_mref, comps_map}, new_monitors} ->
        {Map.keys(comps_map), new_monitors}

      {comps_map, _} when is_map(comps_map) ->
        {Map.keys(comps_map), monitors}

      {nil, _} ->
        {[], monitors}
    end
  end

  def components(target, monitors) do
    case Map.get(monitors, target) do
      {_mref, comps} -> Map.keys(comps)
      comps when is_map(comps) -> Map.keys(comps)
      nil -> []
    end
  end

  defp emit_current_node_state(node) do
    nodes = [node() | Node.list()]

    if node in nodes do
      send(self(), {:nodeup, node, []})
    else
      send(self(), {:nodedown, node, []})
    end
  end
end
