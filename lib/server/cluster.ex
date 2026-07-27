defmodule RaftEx.Server.Cluster do
  @moduledoc """
  Helpers for managing the cluster membership map inside a server state.

  The cluster is a `%{server_id => peer_state}` map stored under `:cluster` in
  the server state.  This module keeps all mutation and query logic in one place
  so that `RaftEx.Server` stays focused on the Raft protocol.
  """

  alias RaftEx.Types

  @type peer_state :: Types.peer_state()
  @type cluster :: Types.cluster()
  @type server_id :: Types.server_id()

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc "Build an initial cluster map from a list or map of peer identifiers."
  @spec new(server_id(), [server_id()] | map()) :: cluster()
  def new(self, peers) when is_list(peers) do
    peers
    |> Enum.reduce(%{}, fn n, acc -> Map.put(acc, n, new_peer()) end)
    |> ensure_self(self)
  end

  def new(self, peers) when is_map(peers) do
    peers
    |> Map.new(fn {k, v} -> {k, merge_peer(v)} end)
    |> ensure_self(self)
  end

  # ---------------------------------------------------------------------------
  # Peer accessors
  # ---------------------------------------------------------------------------

  @doc "Return all peers excluding `self`."
  @spec peers(server_id(), cluster()) :: cluster()
  def peers(self, cluster), do: Map.delete(cluster, self)

  @doc "Return the peer ids excluding `self`."
  @spec peer_ids(server_id(), cluster()) :: [server_id()]
  def peer_ids(self, cluster), do: Map.keys(peers(self, cluster))

  @doc "Get a single peer's state, returning `nil` if not found."
  @spec get_peer(server_id(), cluster()) :: peer_state() | nil
  def get_peer(peer_id, cluster), do: Map.get(cluster, peer_id)

  @doc "Update a peer by merging `update` into its current state."
  @spec update_peer(server_id(), map(), cluster()) :: cluster()
  def update_peer(peer_id, update, cluster) when is_map(update) do
    peer = Map.merge(Map.get(cluster, peer_id, %{}), update)
    Map.put(cluster, peer_id, peer)
  end

  # ---------------------------------------------------------------------------
  # Membership
  # ---------------------------------------------------------------------------

  @doc "Determine the membership role of `id`/`uid` within `cluster`."
  @spec get_membership(cluster(), server_id(), RaftEx.Types.uid(), RaftEx.Types.membership()) ::
          RaftEx.Types.membership()
  def get_membership(cluster, peer_id, uid, default) do
    case cluster do
      %{^peer_id => %{voter_status: %{uid: ^uid} = vs}} ->
        Map.get(vs, :membership, default)

      _ ->
        default
    end
  end

  # ---------------------------------------------------------------------------
  # Match-index quorum helpers
  # ---------------------------------------------------------------------------

  @doc """
  Collect the match indexes of all voter peers plus the leader's own last
  written index.

  Used to compute the new commit index.
  """
  @spec voter_match_indexes(server_id(), cluster(), RaftEx.Types.index()) ::
          [RaftEx.Types.index()]
  def voter_match_indexes(self, cluster, leader_last_written) do
    Enum.reduce(cluster, [leader_last_written], fn
      {^self, _}, acc ->
        acc

      {_, %{voter_status: %{membership: m}}}, acc when m != :voter ->
        acc

      {_, %{match_index: mi}}, acc ->
        [mi | acc]
    end)
  end

  @doc "Return the highest index agreed upon by a quorum (majority)."
  @spec agreed_commit([RaftEx.Types.index()]) :: RaftEx.Types.index()
  def agreed_commit(indexes) do
    sorted = Enum.sort(indexes, :desc)
    quorum = div(length(sorted), 2) + 1
    Enum.at(sorted, quorum - 1)
  end

  # ---------------------------------------------------------------------------
  # Peer state transitions
  # ---------------------------------------------------------------------------

  @doc "Reset all peer statuses to `:normal` (used on becoming follower)."
  @spec reset_peer_statuses(cluster()) :: cluster()
  def reset_peer_statuses(cluster) do
    Map.new(cluster, fn {id, peer} -> {id, Map.put(peer, :status, :normal)} end)
  end

  @doc "Reset query_index to 0 for all peers (used on term change)."
  @spec reset_query_indexes(cluster()) :: cluster()
  def reset_query_indexes(cluster) do
    Map.new(cluster, fn {id, peer} -> {id, Map.put(peer, :query_index, 0)} end)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp ensure_self(cluster, self) do
    if Map.has_key?(cluster, self), do: cluster, else: Map.put(cluster, self, new_peer())
  end

  @doc false
  def new_peer do
    %{
      next_index: 1,
      match_index: 0,
      commit_index_sent: 0,
      query_index: 0,
      status: :normal
    }
  end

  defp merge_peer(existing) do
    Map.merge(new_peer(), existing)
  end
end
