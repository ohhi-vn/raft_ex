defmodule RaftEx.TestSupport.DistributedCluster do
  @moduledoc """
  Helpers for spawning separate BEAM nodes (via `:peer`) running RaftEx, used by
  the `:distributed` test suite. Tests using this module must be tagged
  `:distributed` (excluded by default; run with `mix test --include distributed`).

  Multi-host runs are supported by passing `:hosts` (list of hostnames) instead
  of the default localhost peers.
  """

  @ebin_path Path.expand("_build/test/lib/raft_ex/ebin", File.cwd!())

  alias RaftEx.Domain.Utils, as: Utils

  @doc "Returns the compiled ebin path for raft_ex."
  def ebin_path, do: @ebin_path

  @doc """
  Spawns `n` peer nodes. Each peer has raft_ex on its code path and the app
  started. Returns a list of `{peer, node}` tuples.
  """
  def start_peers(n, opts \\ []) do
    hosts = Keyword.get(opts, :hosts)

    code_paths = :code.get_path() |> Enum.map(&to_charlist/1)

    dep_ebins =
      Path.wildcard(Path.expand("_build/test/lib/*/ebin", File.cwd!()))
      |> Enum.map(&to_charlist/1)

    args =
      Enum.flat_map(code_paths ++ dep_ebins, fn p -> [~c"-pa", p] end)

    for i <- 1..n do
      start_one_peer(i, hosts, args)
    end
  end

  defp start_one_peer(i, hosts, args) do
    name = :"raft_peer_#{i}_#{System.unique_integer([:positive])}"

    host = peer_host(i, hosts)

    peer_opts =
      if host do
        %{name: name, args: args, host: String.to_charlist(host)}
      else
        %{name: name, args: args}
      end

    {:ok, peer, node} = :peer.start(peer_opts)

    {:ok, _} = :rpc.call(node, Application, :ensure_all_started, [:raft_ex])

    data_dir = "/tmp/raft_peer_data_#{i}_#{System.unique_integer([:positive])}"
    :rpc.call(node, File, :mkdir_p!, [data_dir])
    :ok = :rpc.call(node, RaftEx, :start_in, [data_dir])

    {peer, node}
  end

  defp peer_host(_i, nil), do: nil

  defp peer_host(_i, []), do: nil

  defp peer_host(i, hosts) do
    Enum.at(hosts, rem(i - 1, length(hosts)))
  end

  @doc "Stops all peers."
  def stop_peers(peers) do
    for {peer, _node} <- peers do
      try do
        :peer.stop(peer)
      catch
        _, _ -> :ok
      end
    end

    :ok
  end

  @doc """
  On the coordinator node, starts a cluster spanning the peer nodes plus the
  coordinator. Returns `{name, leader, ids}`.
  """
  def start_cluster(peers, machine, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 15_000)

    peer_nodes = Enum.map(peers, fn {_p, n} -> n end)
    # ids: coordinator is not a server by default; all servers live on peers
    ids = Enum.with_index(peer_nodes, 1) |> Enum.map(fn {node, i} -> {:"s#{i}", node} end)

    name = :"dist_#{System.unique_integer([:positive])}"

    # Start each server on its own peer node.
    results =
      Enum.map(ids, fn id ->
        uid = :rpc.call(elem(id, 1), RaftEx, :new_uid, [Utils.to_binary(name)])

        config = %{
          id: id,
          uid: uid,
          cluster_name: name,
          log_init_args: %{uid: uid},
          initial_members: ids,
          machine: machine
        }

        :rpc.call(elem(id, 1), RaftEx, :start_server, [:default, config], timeout)
      end)

    unless Enum.all?(results, &(&1 == :ok)) do
      raise "failed to start servers: #{inspect(results)}"
    end

    # Trigger election.
    first_id = List.first(ids)
    :rpc.call(elem(first_id, 1), RaftEx, :trigger_election, [first_id], 5_000)

    Process.sleep(1_000)

    leader = elect_leader(ids)

    if leader == nil, do: raise("no leader elected")
    {name, leader, ids, peer_nodes}
  end

  defp elect_leader(ids) do
    Enum.reduce_while(1..4, nil, fn attempt, _acc ->
      found = wait_for_leader(ids, 30, 250) || retry_election(ids, attempt)
      if found, do: {:halt, found}, else: {:cont, nil}
    end)
  end

  defp wait_for_leader(ids, attempts, interval) do
    Enum.find_value(1..attempts, fn _ ->
      Process.sleep(interval)
      discover_leader(ids)
    end)
  end

  defp retry_election(ids, attempt) do
    target = Enum.at(ids, rem(attempt - 1, length(ids)))
    :rpc.call(elem(target, 1), RaftEx, :trigger_election, [target], 5_000)
    wait_for_leader(ids, 20, 250)
  end

  # Ping reports the role without appending a command to the log. Using
  # process_command here used to write entries while probing each server, which
  # left the peers with divergent logs and caused election churn.
  defp discover_leader(ids) do
    Enum.find_value(ids, fn id ->
      case :rpc.call(elem(id, 1), RaftEx, :ping, [id, 2_000]) do
        {:pong, :leader} -> id
        _ -> nil
      end
    end)
  end

  @doc "Runs a function on the node owning `id` via RPC."
  def rpc(id, mod, fun, args, timeout \\ 5_000) do
    :rpc.call(elem(id, 1), mod, fun, args, timeout)
  end
end
