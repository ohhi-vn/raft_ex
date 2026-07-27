defmodule RaftEx.Leaderboard do
  @moduledoc """
  In-memory leaderboard for tracking Raft cluster leaders across nodes.

  Provides a lightweight ETS-based registry that records which node is the
  current leader for each Raft cluster. Useful for client-side request routing
  so that commands can be sent directly to the leader.
  """

  @table __MODULE__

  @doc """
  Initialize the leaderboard ETS table. Idempotent.
  """
  def init do
    case :ets.info(@table) do
      :undefined ->
        :ets.new(@table, [:set, :named_table, :public])
        :ok

      _ ->
        :ok
    end
  end

  @doc """
  Record the current leader and members for a cluster.
  """
  @spec record(term(), RaftEx.Types.server_id(), [RaftEx.Types.server_id()]) :: :ok
  def record(cluster_name, leader, members) do
    true = :ets.insert(@table, {cluster_name, leader, members, DateTime.utc_now()})
    :ok
  end

  @doc """
  Clear the leaderboard entry for a cluster.
  """
  @spec clear(term()) :: :ok
  def clear(cluster_name) do
    true = :ets.delete(@table, cluster_name)
    :ok
  end

  @doc """
  Look up the current leader for a cluster.
  """
  @spec lookup_leader(term()) :: RaftEx.Types.server_id() | nil
  def lookup_leader(cluster_name) do
    case lookup(cluster_name) do
      {_, leader, _, _} -> leader
      _ -> nil
    end
  end

  @doc """
  Look up cluster members.
  """
  @spec lookup_members(term()) :: [RaftEx.Types.server_id()] | nil
  def lookup_members(cluster_name) do
    case lookup(cluster_name) do
      {_, _, members, _} -> members
      _ -> nil
    end
  end

  @doc """
  Get the last recorded timestamp for a cluster.
  """
  @spec lookup_timestamp(term()) :: DateTime.t() | nil
  def lookup_timestamp(cluster_name) do
    case lookup(cluster_name) do
      {_, _, _, ts} -> ts
      _ -> nil
    end
  end

  @doc """
  Return all known leaderboard entries.
  """
  @spec overview() :: list()
  def overview do
    :ets.tab2list(@table)
    |> Enum.map(fn {name, leader, members, ts} ->
      %{
        cluster: name,
        leader: leader,
        members: members,
        recorded_at: ts
      }
    end)
  end

  @doc """
  Find the local server ID for a given cluster.

  This is useful for routing requests to the local Raft server process.
  """
  @spec local_server_id(term()) :: RaftEx.Types.server_id() | nil
  def local_server_id(cluster_name) do
    case lookup_members(cluster_name) do
      nil ->
        nil

      members ->
        Enum.find(members, fn {_name, n} -> n == node() end)
    end
  end

  defp lookup(cluster_name) do
    try do
      case :ets.lookup(@table, cluster_name) do
        [record] -> record
        [] -> nil
      end
    rescue
      ArgumentError -> nil
    end
  end
end
