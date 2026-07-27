defmodule RaftEx.ClusterSetup do
  @moduledoc """
  Convenience helpers for bootstrapping distributed RaftEx clusters across
  multiple Elixir nodes.

  Use `setup/3` or `setup/4` to initialise distribution, start the RaftEx
  system, and form a cluster in a single call.
  """

  require Logger

  @doc """
  Full cluster bootstrap on the current node.

  ## Parameters

    * `cluster_name` — atom identifying the Raft cluster
    * `machine` — state machine config `{:machine, Module, args}`
    * `node_configs` — list of `%{node: node(), data_dir: binary()}` maps, one
      per participating node.  The caller determines the data-dir for each node.
    * `opts` — keyword overrides (see `RaftEx.Cluster.form_cluster/4`).

  Returns `{:ok, local_server_ids, all_server_ids}`.
  """
  @spec setup(
          cluster_name :: term(),
          machine :: RaftEx.Machine.machine(),
          node_configs :: [map()],
          opts :: keyword()
        ) :: {:ok, [RaftEx.Types.server_id()], [RaftEx.Types.server_id()]} | {:error, term()}
  def setup(cluster_name, machine, node_configs, opts \\ []) do
    nodes = Enum.map(node_configs, & &1.node)

    this_config = Enum.find(node_configs, fn c -> c.node == node() end)

    self_node = node()
    all_nodes = Enum.sort(nodes)

    start_distribution(Keyword.get(opts, :cookie, :raft_ex))

    if this_config do
      start_system(this_config.data_dir)
    else
      start_system("/tmp/raft_ex/#{self_node}")
    end

    case RaftEx.Cluster.form_cluster(cluster_name, machine, all_nodes, opts) do
      {:ok, local_ids} ->
        all_ids = RaftEx.Distribution.build_server_ids(all_nodes)
        Logger.info("RaftEx.ClusterSetup: cluster #{inspect(cluster_name)} ready")
        {:ok, local_ids, all_ids}

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Start distributed Erlang with the given cookie.
  """
  def start_distribution(cookie \\ :raft_ex) do
    Node.set_cookie(cookie)

    case Node.start(:hidden) do
      {:ok, pid} when is_pid(pid) ->
        Logger.info("RaftEx.ClusterSetup: distribution started as #{inspect(node())}")
        pid

      {:error, :already_started} ->
        :ok

      {:error, reason} ->
        Logger.error("RaftEx.ClusterSetup: distribution failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Start the RaftEx system with the given data directory.
  """
  def start_system(data_dir) do
    File.mkdir_p!(data_dir)
    RaftEx.start_in(data_dir)
  end

  @doc """
  Connect to a remote node and verify it is reachable.
  """
  def connect_node(node, cookie \\ :raft_ex) do
    Node.set_cookie(cookie)

    case Node.connect(node) do
      true ->
        Logger.info("RaftEx.ClusterSetup: connected to #{inspect(node)}")
        :ok

      false ->
        Logger.warning("RaftEx.ClusterSetup: could not connect to #{inspect(node)}")
        {:error, :connection_failed}
    end
  end

  @doc """
  Verify that all given nodes are reachable and running RaftEx.
  """
  def verify_nodes(nodes, timeout \\ 5_000) do
    results =
      Enum.map(nodes, fn n ->
        reachable = n == node() or Node.connect(n)

        raft_running =
          if reachable do
            :rpc.call(n, :persistent_term, :get, [{:"$ra_system", :default}, nil], timeout)
          else
            nil
          end

        %{node: n, reachable: reachable, raft_running: raft_running != nil}
      end)

    failed = Enum.reject(results, &(&1.reachable and &1.raft_running))

    if failed == [] do
      :ok
    else
      {:error, {:nodes_not_ready, failed}}
    end
  end
end
