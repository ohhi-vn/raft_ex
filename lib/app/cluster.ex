defmodule RaftEx.App.Cluster do
  @moduledoc """
  High-level API for managing distributed RaftEx clusters.
  """

  require Logger

  alias RaftEx.Infra.Network.Distribution
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
  @spec form_cluster(term(), RaftEx.Domain.Machine.machine(), [node()], keyword()) ::
          {:ok, [RaftEx.Domain.Types.server_id()]} | {:error, term()}
  def form_cluster(cluster_name, machine, nodes, opts \\ []) do
    system = Keyword.get(opts, :system, :default)
    timeout = Keyword.get(opts, :timeout, 15_000)
    seed_node = Keyword.get(opts, :seed_node, hd(Enum.sort(nodes)))

    server_ids = Distribution.build_server_ids(nodes)
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
  @spec join_cluster(term(), RaftEx.Domain.Machine.machine(), [node()], node(), keyword()) ::
          {:ok, [RaftEx.Domain.Types.server_id()]} | {:error, term()}
  def join_cluster(cluster_name, machine, all_nodes, seed_node, opts \\ []) do
    system = Keyword.get(opts, :system, :default)
    timeout = Keyword.get(opts, :timeout, 15_000)

    ensure_distribution_started!()

    server_ids = Distribution.build_server_ids(all_nodes)
    local_servers = Enum.filter(server_ids, fn {_, n} -> n == node() end)

    # Nothing to start or add locally: joining from a node that hosts no
    # servers is a no-op, even when the seed node is not reachable.
    if local_servers == [] do
      Logger.warning("RaftEx.App.Cluster: no local servers to join cluster")
      {:ok, []}
    else
      do_join(system, cluster_name, machine, server_ids, local_servers, seed_node, timeout)
    end
  end

  defp do_join(system, cluster_name, machine, server_ids, local_servers, seed_node, timeout) do
    seed_leader = find_any_server_on(seed_node, server_ids)

    # Verify the seed is reachable before starting anything locally.
    if seed_leader != nil and not server_reachable?(seed_leader) do
      Logger.warning("RaftEx.App.Cluster: seed unreachable #{inspect(seed_node)}")
      {:error, :seed_unreachable}
    else
      for sid <- local_servers do
        RaftEx.start_server(system, cluster_name, sid, machine, server_ids)
      end

      :timer.sleep(200)

      if seed_leader == nil do
        Logger.warning("RaftEx.App.Cluster: no server found on seed node #{inspect(seed_node)}")
        {:ok, local_servers}
      else
        add_local_members(seed_leader, local_servers, timeout)
        wait_for_membership(seed_leader, local_servers, timeout)
        {:ok, local_servers}
      end
    end
  end

  defp server_reachable?(server_id) do
    case RaftEx.ping(server_id, 2_000) do
      {:pong, _} ->
        true

      # The node is up but nobody has started the seed server yet. That is the
      # normal case when joining a brand-new single-node cluster, so let the
      # join proceed and start the local servers below.
      {:error, :noproc} ->
        true

      other ->
        Logger.warning("RaftEx.App.Cluster: seed ping #{inspect(server_id)} -> #{inspect(other)}")
        false
    end
  end

  defp wait_for_membership(_seed_leader, [], _timeout), do: :ok

  defp wait_for_membership(seed_leader, local_servers, timeout) when timeout <= 0 do
    _ = seed_leader
    _ = local_servers
    :ok
  end

  defp wait_for_membership(seed_leader, local_servers, timeout) do
    case RaftEx.members(seed_leader, 2_000) do
      {:ok, members, _leader} ->
        if Enum.all?(local_servers, &(&1 in members)) do
          :ok
        else
          Process.sleep(200)
          wait_for_membership(seed_leader, local_servers, timeout - 200)
        end

      _ ->
        Process.sleep(200)
        wait_for_membership(seed_leader, local_servers, timeout - 200)
    end
  end

  defp add_local_members(seed_leader, local_servers, timeout) do
    for sid <- local_servers do
      case RaftEx.add_member(seed_leader, sid, timeout) do
        {:ok, _, leader} ->
          Logger.info(
            "RaftEx.App.Cluster: joined cluster as #{inspect(sid)}, leader: #{inspect(leader)}"
          )

        {:error, reason} ->
          Logger.warning("RaftEx.App.Cluster: failed to add #{inspect(sid)}: #{inspect(reason)}")
      end
    end
  end

  @doc """
  Handle a topology change event from NodeDiscovery.

  When new nodes are detected, attempts to add them as cluster members.
  """
  @spec handle_topology_change([node()], term(), RaftEx.Domain.Machine.machine()) :: :ok
  def handle_topology_change(nodes, cluster_name, machine) do
    system = :default
    server_ids = Distribution.build_server_ids(nodes)
    local_servers = Enum.filter(server_ids, fn {_, n} -> n == node() end)
    existing_members = fetch_existing_members(local_servers)

    missing_servers =
      Enum.filter(server_ids, fn sid -> sid not in existing_members end)

    if missing_servers != [] and local_servers != [] do
      Logger.info(
        "RaftEx.App.Cluster: topology change - #{length(missing_servers)} new servers: #{inspect(missing_servers)}"
      )

      add_missing_members(
        system,
        cluster_name,
        machine,
        missing_servers,
        local_servers,
        server_ids
      )
    end

    :ok
  end

  defp fetch_existing_members([]), do: []

  defp fetch_existing_members([first_local | _]) do
    case RaftEx.members(first_local, 2_000) do
      {:ok, members, _leader} -> members
      _ -> []
    end
  end

  defp add_missing_members(
         system,
         cluster_name,
         machine,
         missing_servers,
         local_servers,
         server_ids
       ) do
    for new_id <- missing_servers do
      add_missing_member(system, cluster_name, machine, new_id, hd(local_servers), server_ids)
    end
  end

  defp add_missing_member(system, cluster_name, machine, new_id, local_id, server_ids) do
    case RaftEx.add_member(local_id, new_id, 5_000) do
      {:ok, {:error, :already_member}, leader} ->
        Logger.info(
          "RaftEx.App.Cluster: #{inspect(new_id)} already a member, leader: #{inspect(leader)}"
        )

        if leader, do: Leaderboard.record(cluster_name, leader, server_ids)

      {:ok, _, leader} ->
        Logger.info("RaftEx.App.Cluster: added #{inspect(new_id)}, leader: #{inspect(leader)}")

        if leader, do: Leaderboard.record(cluster_name, leader, server_ids)

      {:error, :already_member} ->
        Logger.info("RaftEx.App.Cluster: #{inspect(new_id)} already a member")

      {:timeout, _} ->
        Logger.warning("RaftEx.App.Cluster: timeout adding #{inspect(new_id)}")
        start_local_if_needed(system, cluster_name, machine, new_id, server_ids)

      {:error, reason} ->
        Logger.warning("RaftEx.App.Cluster: error adding #{inspect(new_id)}: #{inspect(reason)}")

        start_local_if_needed(system, cluster_name, machine, new_id, server_ids)
    end
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
  @spec find_leader(term()) :: {:ok, RaftEx.Domain.Types.server_id()} | {:error, term()}
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
      form_on_seed_host(system, cluster_name, machine, all_server_ids, local_servers, timeout)
    else
      Enum.each(local_servers, fn sid ->
        RaftEx.start_server(system, cluster_name, sid, machine, all_server_ids)
      end)

      {:ok, local_servers}
    end
  end

  defp form_on_seed_host(system, cluster_name, machine, all_server_ids, local_servers, timeout) do
    seed_id = hd(all_server_ids)

    case RaftEx.start_server(system, cluster_name, seed_id, machine, all_server_ids) do
      {:error, reason} ->
        {:error, reason}

      :ok ->
        form_seed_after_start(
          system,
          cluster_name,
          machine,
          all_server_ids,
          local_servers,
          timeout,
          seed_id
        )
    end
  end

  defp form_seed_after_start(
         system,
         cluster_name,
         machine,
         all_server_ids,
         local_servers,
         timeout,
         seed_id
       ) do
    # Only the seed is started at this point. Waiting for every server here
    # would wait for local servers that are started below (and for remote
    # servers that start themselves via their own form_cluster call), so it
    # could never succeed and made the timeout depend on leftovers from
    # previous tests.
    with :ok <- wait_for_servers([seed_id], timeout),
         :ok <- trigger_election_safe(seed_id, timeout) do
      Enum.each(tl(local_servers), fn sid ->
        RaftEx.start_server(system, cluster_name, sid, machine, all_server_ids)
      end)

      # Remote nodes may still be forming their local servers; give them the
      # remaining budget without making local formation fail if they are slow.
      _ = wait_for_servers(all_server_ids -- local_servers, timeout)

      wait_for_leader(cluster_name, timeout, all_server_ids)
      {:ok, local_servers}
    end
  end

  defp trigger_election_safe(seed_id, timeout) do
    case RaftEx.trigger_election(seed_id, timeout) do
      :ok -> :ok
      other -> {:error, other}
    end
  catch
    :exit, {:timeout, _} -> {:error, {:timeout, seed_id}}
    :exit, reason -> {:error, reason}
  end

  defp wait_for_servers(all_server_ids, timeout) when timeout <= 0 do
    missing =
      Enum.reject(all_server_ids, fn id ->
        case RaftEx.ping(id, 1_000) do
          {:pong, _} -> true
          _ -> false
        end
      end)

    if missing == [], do: :ok, else: {:error, {:servers_not_ready, missing}}
  end

  defp wait_for_servers(all_server_ids, timeout) do
    missing =
      Enum.reject(all_server_ids, fn id ->
        case RaftEx.ping(id, 1_000) do
          {:pong, _} -> true
          _ -> false
        end
      end)

    if missing == [] do
      :ok
    else
      Process.sleep(200)
      wait_for_servers(all_server_ids, timeout - 200)
    end
  end

  defp wait_for_leader(_cluster_name, timeout, _all_server_ids) when timeout <= 0 do
    :ok
  end

  defp wait_for_leader(cluster_name, timeout, all_server_ids) do
    seed_id = List.first(all_server_ids)

    case RaftEx.members(seed_id, 2_000) do
      {:ok, _members, leader} when not is_nil(leader) ->
        Leaderboard.record(cluster_name, leader, all_server_ids)
        :ok

      _ ->
        Process.sleep(100)
        wait_for_leader(cluster_name, timeout - 100, all_server_ids)
    end
  end

  defp start_local_if_needed(system, cluster_name, machine, server_id, all_server_ids) do
    if elem(server_id, 1) == node() do
      RaftEx.start_server(system, cluster_name, server_id, machine, all_server_ids)
    end
  end

  defp find_leader_via_members(cluster_name) do
    members =
      case Leaderboard.lookup_members(cluster_name) do
        nil -> local_cluster_members(cluster_name)
        m -> m
      end

    case members do
      nil ->
        {:error, :unknown_cluster}

      [] ->
        {:error, :unknown_cluster}

      members ->
        retry_find_leader(members, 5)
    end
  end

  defp retry_find_leader(_members, 0) do
    {:error, :no_leader_found}
  end

  defp retry_find_leader(members, attempts) do
    case find_leader_in_members(members) do
      nil ->
        Process.sleep(200)
        retry_find_leader(members, attempts - 1)

      leader ->
        {:ok, leader}
    end
  end

  defp find_leader_in_members(members) do
    Enum.find_value(members, fn member ->
      case RaftEx.members(member, 2_000) do
        {:ok, _, leader} -> leader
        _ -> nil
      end
    end)
  end

  defp local_cluster_members(cluster_name) do
    case RaftEx.overview(:default) do
      :system_not_started ->
        nil

      overview ->
        ids =
          overview.servers
          |> Enum.filter(fn {_name, info} -> info.cluster == cluster_name end)
          |> Enum.map(fn {name, _info} -> {name, node()} end)

        if ids == [], do: nil, else: ids
    end
  end

  defp ensure_distribution_started! do
    case Node.start(:hidden) do
      {:ok, _} ->
        :ok

      {:error, :already_started} ->
        :ok

      {:error, {:already_started, _}} ->
        :ok

      {:error, reason} ->
        Logger.warning("RaftEx.App.Cluster: node.start failed: #{inspect(reason)}")
        :ok
    end
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp find_any_server_on(node, server_ids) do
    Enum.find(server_ids, fn {_name, n} -> n == node end)
  end
end
