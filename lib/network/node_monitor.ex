defmodule RaftEx.NodeMonitor do
  @moduledoc """
  Monitors cluster node connectivity and notifies Raft server processes
  of node up/down events.

  Started as part of the RaftEx supervision tree. Ensures that Raft servers
  react promptly to topology changes in the Elixir cluster.
  """

  use GenServer
  require Logger

  def start_link(opts \\ []) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(_opts) do
    if Node.alive?() do
      Node.monitor(:all, true)
      Logger.info("RaftEx.NodeMonitor: started monitoring cluster nodes")
    else
      Logger.warning("RaftEx.NodeMonitor: node not alive, monitoring deferred")
    end

    {:ok, %{alive: Node.alive?()}}
  end

  @impl GenServer
  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("RaftEx.NodeMonitor: node up: #{inspect(node)}")
    RaftEx.Network.handle_node_up(node)
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Logger.warning("RaftEx.NodeMonitor: node down: #{inspect(node)}")
    RaftEx.Network.handle_node_down(node)
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}
end
