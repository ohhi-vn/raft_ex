defmodule RaftEx.Distribution do
  @moduledoc """
  Helpers for setting up distributed Erlang for RaftEx clusters.
  """

  require Logger

  @doc """
  Start distributed Erlang for the current node.
  """
  def start(opts \\ []) do
    cookie = Access.get(opts, :cookie, :raft_ex)
    Node.set_cookie(cookie)

    case Node.start(:hidden) do
      {:ok, pid} when is_pid(pid) ->
        Logger.info("RaftEx.Distribution: started hidden node #{inspect(node())}")
        pid

      {:error, :already_started} ->
        Logger.debug("RaftEx.Distribution: node already started")
        :ok

      {:error, reason} ->
        Logger.error("RaftEx.Distribution: failed to start node: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Connect to a list of seed nodes.
  """
  @spec connect_to_seeds([node()], atom()) :: [node()]
  def connect_to_seeds(seeds, cookie \\ :raft_ex) do
    unless Node.alive?() do
      Logger.warning("RaftEx.Distribution: node not alive, skipping seed connection")
      []
    else
      try do
        Node.set_cookie(cookie)
      rescue
        _ -> :ok
      end

      seeds
      |> Enum.filter(&(&1 != node()))
      |> Enum.filter(fn seed ->
        try do
          case Node.connect(seed) do
            true ->
              Logger.info("RaftEx.Distribution: connected to seed #{inspect(seed)}")
              true

            false ->
              Logger.warning("RaftEx.Distribution: failed to connect to seed #{inspect(seed)}")
              false
          end
        rescue
          _ ->
            Logger.warning("RaftEx.Distribution: error connecting to seed #{inspect(seed)}")
            false
        end
      end)
    end
  end

  @doc """
  Return all visible cluster nodes (including self).
  """
  @spec cluster_nodes() :: [node()]
  def cluster_nodes do
    [node() | Node.list()]
  end

  @doc """
  Build deterministic server IDs from a list of nodes.

  Nodes are sorted so every node in the cluster produces the same mapping
  regardless of the order in which they were supplied.
  """
  @spec build_server_ids([node()], atom()) :: [RaftEx.Types.server_id()]
  def build_server_ids(nodes, prefix \\ :server) do
    nodes
    |> Enum.sort()
    |> Enum.with_index(1)
    |> Enum.map(fn {n, i} -> {String.to_atom("#{prefix}#{i}"), n} end)
  end
end
