defmodule RaftEx.Cluster do
  @moduledoc """
  High-level API for managing distributed RaftEx clusters.
  """

  require Logger

  alias RaftEx.Leaderboard

  @doc """
  Form a distributed Raft cluster across multiple nodes.

  This is the primary entry point for multi-node cluster formation.
  Each node should call this with the same set of participating nodes.

  ## Parameters
    * `cluster_name` - name for this Raft cluster
    * `machine` - the state machine configuration `{:machine, Module, args}`
    * `nodes` - list of participating Erlang nodes (including self)
    * `opts` - optional keyword list:
      - `:system` - RaftEx system name (default: `:default`)
      - `:timeout` - timeout in ms (default: 10_000)
      - `:seed_node` - node to act as seed (default: first node in sorted list)

  Returns `{:ok, local_server_ids}` or `{:error, reason}`.
  """
  @spec form_cluster(term(), RaftEx.Machine.machine(), [node()], keyword()) ::
          {:ok, [RaftEx.Types.server_id()]} | {:error, term()}
  def form_cluster(cluster_name, machine, nodes, opts \\ []) do
    system = Keyword.get(opts, :system, :default)
    timeout = Keyword.get(opts, :timeout, 10_000)
    seed_node = Keyword.get(opts, :seed_node, hd(Enum.sort(nodes)))

    server_ids = RaftEx.Distribution.build_server_ids(nodes)
    local_servers = Enum.filter(server_ids, fn {_, n} -> n == node() end)

    if local_servers == [] do
      {:error, :no_local_servers}
    else
      ensure_distribution_started!()
      do_form(system, cluster_name, machine, server_ids, local_servers, seed_node, timeout)
    end
  end

  @doc """
  Join an existing Raft cluster as a new node.
  """
  @spec join_cluster(term(), RaftEx.Machine.machine(), [node()], node(), keyword()) ::
          {:ok, [RaftEx.Types.server_id()]} | {:error, term()}
  def join_cluster(cluster_name, machine, all_nodes, seed_node, opts \\ []) do
    system = Keyword.get(opts, :system, :default)
    timeout = Keyword.get(opts, :timeout, 10_000)

    ensure_distribution_started!()

    server_ids = RaftEx.Distribution.build_server_ids(all_nodes)
    local_servers = Enum.filter(server_ids, fn {_, n} -> n == node() end)

    for sid <- local_servers do
      RaftEx.start_server(system, cluster_name, sid, machine, server_ids)
    end

    :timer.sleep(200)

    seed_leader = find_any_server_on(seed_node, server_ids)

    if seed_leader == nil do
      Logger.warning("RaftEx.Cluster: no server found on seed node #{inspect(seed_node)}")
    else
      for sid <- local_servers do
        case RaftEx.add_member(seed_leader, sid, timeout) do
          {:ok, _, leader} ->
            Logger.info("RaftEx.Cluster: joined cluster as #{inspect(sid)}, leader: #{inspect(leader)}")

          {:error, reason} ->
            Logger.warning("RaftEx.Cluster: failed to add #{inspect(sid)}: #{inspect(reason)}")
        end
      end
    end

    {:ok, local_servers}
  end

  @doc """
  Handle a topology change event from NodeDiscovery.

  When new nodes are detected, attempts to add them as cluster members.
  """
  @spec handle_topology_change([node()], term(), RaftEx.Machine.machine()) :: :ok
  def handle_topology_change(nodes, cluster_name, machine) do
    system = :default
    server_ids = RaftEx.Distribution.build_server_ids(nodes)
    local_servers = Enum.filter(server_ids, fn {_, n} -> n == node() end)

    existing_members =
      case local_servers do
        [first_local | _] ->
          case RaftEx.members(first_local, 2_000) do
            {:ok, members, _leader} -> members
            _ -> []
          end

        [] ->
          []
      end

    missing_servers =
      Enum.filter(server_ids, fn sid -> sid not in existing_members end)

    if missing_servers != [] and local_servers != [] do
      Logger.info(
        "RaftEx.Cluster: topology change - #{length(missing_servers)} new servers: #{inspect(missing_servers)}"
      )

      for new_id <- missing_servers do
        case RaftEx.add_member(hd(local_servers), new_id, 5_000) do
          {:ok, _, leader} ->
            Logger.info("RaftEx.Cluster: added #{inspect(new_id)}, leader: #{inspect(leader)}")
            Leaderboard.record(cluster_name, leader, server_ids)

          {:timeout, _} ->
            Logger.warning("RaftEx.Cluster: timeout adding #{inspect(new_id)}")
            start_local_if_needed(system, cluster_name, machine, new_id, server_ids)

          {:error, reason} ->
            Logger.warning("RaftEx.Cluster: error adding #{inspect(new_id)}: #{inspect(reason)}")
            start_local_if_needed(system, cluster_name, machine, new_id, server_ids)
        end
      end
    end

    :ok
  end

  @doc """
  Get the status of all RaftEx servers on this node.
  """
  @spec status(atom()) :: {:ok, list()} | {:error, term()}
  def status(system \\ :default) do
    case RaftEx.overview(system) do
      :system_not_started ->
        {:error, :system_not_started}

      overview ->
        servers =
          overview.servers
          |> Enum.map(fn {name, info} ->
            %{
              server: name,
              state: info.state,
              membership: info.membership,
              cluster: info.cluster_name,
              pid: info.pid,
              snapshot: info.snapshot_state
            }
          end)

        {:ok, %{node: node(), servers: servers}}
    end
  end

  @doc """
  Find the current leader for a given cluster name.

  First checks the local Leaderboard, then queries a member server.
  """
  @spec find_leader(term()) :: {:ok, RaftEx.Types.server_id()} | {:error, term()}
  def find_leader(cluster_name) do
    case Leaderboard.lookup_leader(cluster_name) do
      nil ->
        find_leader_via_members(cluster_name)

      leader ->
        {:ok, leader}
    end
  end

  defp do_form(system, cluster_name, machine, all_server_ids, local_servers, _seed_node, timeout) do
    if hd(all_server_ids) in local_servers do
      seed_id = hd(all_server_ids)

      case RaftEx.start_server(system, cluster_name, seed_id, machine, all_server_ids) do
        :ok ->
          try do
            RaftEx.trigger_election(seed_id, timeout)
          catch
            :exit, {:timeout, _} ->
              {:error, {:timeout, seed_id}}
          else
            :ok ->
              for sid <- tl(local_servers) do
                RaftEx.start_server(system, cluster_name, sid, machine, all_server_ids)
              end

              wait_for_leader(cluster_name, timeout)

              {:ok, local_servers}
          end

        {:error, reason} ->
          {:error, reason}
      end
    else
      for sid <- local_servers do
        RaftEx.start_server(system, cluster_name, sid, machine, all_server_ids)
      end

      {:ok, local_servers}
    end
  end

  defp wait_for_leader(_cluster_name, timeout) when timeout <= 0 do
    :ok
  end

  defp wait_for_leader(cluster_name, timeout) do
    case Leaderboard.lookup_leader(cluster_name) do
      nil ->
        Process.sleep(100)
        wait_for_leader(cluster_name, timeout - 100)

      _leader ->
        :ok
    end
  end

  defp start_local_if_needed(system, cluster_name, machine, server_id, all_server_ids) do
    if elem(server_id, 1) == node() do
      RaftEx.start_server(system, cluster_name, server_id, machine, all_server_ids)
    end
  end

  defp find_leader_via_members(cluster_name) do
    case Leaderboard.lookup_members(cluster_name) do
      nil ->
        {:error, :unknown_cluster}

      members ->
        result =
          Enum.find_value(members, fn member ->
            case RaftEx.members(member, 2_000) do
              {:ok, _, leader} -> leader
              _ -> nil
            end
          end)

        case result do
          nil -> {:error, :no_leader_found}
          leader -> {:ok, leader}
        end
    end
  end

  defp ensure_distribution_started! do
    try do
      case Node.start(:hidden) do
        {:ok, _} -> :ok
        {:error, :already_started} -> :ok
        {:error, {:already_started, _}} -> :ok
        {:error, reason} ->
          Logger.warning("RaftEx.Cluster: node.start failed: #{inspect(reason)}")
          :ok
      end
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp find_any_server_on(node, server_ids) do
    Enum.find(server_ids, fn {_name, n} -> n == node end)
  end
end
