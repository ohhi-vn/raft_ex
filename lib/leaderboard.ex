defmodule RaftEx.Leaderboard do
  @table __MODULE__

  def init do
    :ets.new(@table, [:set, :named_table, :public])
    :ok
  end

  def record(cluster_name, leader, members) do
    true = :ets.insert(@table, {cluster_name, leader, members})
    :ok
  end

  def clear(cluster_name) do
    true = :ets.delete(@table, cluster_name)
    :ok
  end

  def lookup_leader(cluster_name) do
    case lookup(cluster_name) do
      {_, leader, _} -> leader
      _ -> nil
    end
  end

  def lookup_members(cluster_name) do
    case lookup(cluster_name) do
      {_, _, members} -> members
      _ -> nil
    end
  end

  def overview, do: :ets.tab2list(@table)

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
