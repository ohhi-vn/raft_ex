defmodule RaftEx.NodeDiscovery do
  @moduledoc """
  Seed-based node discovery for RaftEx clusters.
  """

  use GenServer
  require Logger

  @default_discovery_interval 5_000

  defstruct [
    :seeds,
    :cookie,
    :discovery_interval,
    :cluster_name,
    :machine,
    :callback_module,
    :timer_ref
  ]

  @doc """
  Start the node discovery process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(opts) do
    seeds = Keyword.fetch!(opts, :seeds)
    cookie = opts[:cookie] || :raft_ex
    discovery_interval = opts[:discovery_interval] || @default_discovery_interval
    cluster_name = opts[:cluster_name]
    machine = opts[:machine]
    callback_module = opts[:callback_module] || RaftEx.Cluster

    RaftEx.Distribution.start(cookie: cookie)
    connected = RaftEx.Distribution.connect_to_seeds(seeds, cookie)

    timer_ref = schedule_discovery(discovery_interval)

    state = %__MODULE__{
      seeds: seeds,
      cookie: cookie,
      discovery_interval: discovery_interval,
      cluster_name: cluster_name,
      machine: machine,
      callback_module: callback_module,
      timer_ref: timer_ref
    }

    if connected != [] and cluster_name != nil and machine != nil do
      callback_module.handle_topology_change(connected, cluster_name, machine)
    end

    Logger.info(
      "RaftEx.NodeDiscovery: started with #{length(seeds)} seeds, #{length(connected)} connected"
    )

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:discover, state) do
    previous_nodes = RaftEx.Distribution.cluster_nodes()

    _connected = RaftEx.Distribution.connect_to_seeds(state.seeds, state.cookie)

    current_nodes = RaftEx.Distribution.cluster_nodes()

    new_nodes = current_nodes -- previous_nodes
    gone_nodes = previous_nodes -- current_nodes

    if new_nodes != [] or gone_nodes != [] do
      Logger.info(
        "RaftEx.NodeDiscovery: topology change - new: #{inspect(new_nodes)}, gone: #{inspect(gone_nodes)}"
      )

      if state.cluster_name != nil and state.machine != nil do
        state.callback_module.handle_topology_change(
          current_nodes,
          state.cluster_name,
          state.machine
        )
      end
    end

    timer_ref = schedule_discovery(state.discovery_interval)
    {:noreply, %{state | timer_ref: timer_ref}}
  end

  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("RaftEx.NodeDiscovery: node up: #{inspect(node)}")

    if state.cluster_name != nil and state.machine != nil do
      state.callback_module.handle_topology_change(
        RaftEx.Distribution.cluster_nodes(),
        state.cluster_name,
        state.machine
      )
    end

    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Logger.info("RaftEx.NodeDiscovery: node down: #{inspect(node)}")

    if state.cluster_name != nil and state.machine != nil do
      state.callback_module.handle_topology_change(
        RaftEx.Distribution.cluster_nodes(),
        state.cluster_name,
        state.machine
      )
    end

    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp schedule_discovery(interval) do
    Process.send_after(self(), :discover, interval)
  end
end
