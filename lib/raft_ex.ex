defmodule RaftEx do
  @moduledoc """
  Primary module for interacting with RaftEx servers and clusters.
  """

  require Logger

  @default_timeout 5_000

  @type index :: non_neg_integer()
  @type idxterm :: {index(), non_neg_integer()}
  @type server_id :: {atom(), node()}
  @type cluster_name :: binary() | String.t() | atom()
  @type uid :: binary()
  @type membership :: :voter | :promotable | :non_voter | :unknown

  @type cmd_ret ::
          {:ok, reply :: term(), leader :: server_id()}
          | {:error, term()}
          | {:timeout, server_id()}

  # ---- startup / teardown -------------------------------------------

  def start(params \\ []) do
    Application.stop(:ra)
    Application.load(:ra)
    Enum.each(params, fn {key, val} -> Application.put_env(:ra, key, val) end)
    res = Application.ensure_all_started(:ra)
    RaftEx.System.start_default()
    res
  end

  def start_in(data_dir) do
    start([{:data_dir, data_dir}])
  end

  # ---- cluster management ------------------------------------------

  def start_cluster(system, cluster_name, machine, server_ids, timeout \\ @default_timeout)
      when is_atom(system) do
    configs =
      Enum.map(server_ids, fn id ->
        uid = new_uid(RaftEx.Lib.to_binary(cluster_name))

        %{
          id: id,
          uid: uid,
          cluster_name: cluster_name,
          log_init_args: %{uid: uid},
          initial_members: server_ids,
          machine: machine
        }
      end)

    start_cluster(system, configs, timeout)
  end

  def start_cluster(system, [%{cluster_name: cluster_name} | _] = configs, timeout)
      when is_atom(system) do
    result =
      RaftEx.Lib.partition_parallel(
        fn c ->
          case start_server(system, c) do
            :ok ->
              true

            err ->
              Logger.error("ra: failed to start a server #{inspect(c)}, error: #{inspect(err)}")
              false
          end
        end,
        configs
      )

    case result do
      {:ok, [], _not_started} ->
        Logger.error("ra: failed to form cluster #{inspect(cluster_name)}. No servers started.")
        {:error, :cluster_not_formed}

      {:ok, started, not_started} ->
        started_ids = Enum.map(started, & &1.id)
        not_started_ids = Enum.map(not_started, & &1.id)

        triggered_id =
          Enum.find_value(started_ids, fn n ->
            if trigger_election(n) == :ok, do: n
          end)

        case members(triggered_id, length(configs) * timeout) do
          {:ok, _, leader} ->
            Logger.info(
              "ra: started cluster #{inspect(cluster_name)} with #{length(configs)} servers. " <>
                "#{length(not_started)} failed. Leader: #{inspect(leader)}"
            )

            {:ok, started_ids, not_started_ids}

          err ->
            Logger.warning("ra: failed to form cluster #{inspect(cluster_name)}: #{inspect(err)}")
            Enum.each(started_ids, &force_delete_server(system, &1))
            {:error, :cluster_not_formed}
        end

      {:error, {:partition_parallel_timeout, started, _}} ->
        started_ids = Enum.map(started, & &1.id)
        Enum.each(started_ids, &force_delete_server(system, &1))
        {:error, :cluster_not_formed}
    end
  end

  def start_or_restart_cluster(
        system,
        cluster_name,
        machine,
        server_ids,
        timeout \\ @default_timeout
      ) do
    case RaftEx.ServerSupSupervisor.restart_server(system, hd(server_ids), %{}) do
      {:ok, _} ->
        Enum.each(tl(server_ids), &RaftEx.ServerSupSupervisor.restart_server(system, &1, %{}))
        {:ok, server_ids, []}

      {:error, _err} ->
        start_cluster(system, cluster_name, machine, server_ids, timeout)
    end
  end

  def delete_cluster(server_ids, timeout \\ @default_timeout) do
    cmd = {:"$ra_cluster", :delete, :await_consensus}

    case RaftEx.ServerProc.command(server_ids, cmd, timeout) do
      {:ok, _, leader} -> {:ok, leader}
      {:timeout, _} -> {:error, :timeout}
      err -> err
    end
  end

  # ---- server management -------------------------------------------

  def start_server(system, config) when is_atom(system) do
    uid = Map.get(config, :uid)

    if RaftEx.Lib.validate_base64uri(uid) do
      case RaftEx.ServerSupSupervisor.start_server(system, config) do
        {:ok, _} -> :ok
        {:ok, _, _} -> :ok
        {:error, _} = err -> err
        err -> {:error, err}
      end
    else
      {:error, :invalid_uid}
    end
  end

  def start_server(system, cluster_name, %{id: {_, _}} = conf0, machine, server_ids)
      when is_atom(system) do
    uid = Map.get(conf0, :uid, new_uid(RaftEx.Lib.to_binary(cluster_name)))

    conf =
      Map.merge(conf0, %{
        cluster_name: cluster_name,
        uid: uid,
        initial_members: server_ids,
        log_init_args: %{uid: uid},
        machine: machine
      })

    start_server(system, conf)
  end

  def start_server(system, cluster_name, {_, _} = server_id, machine, server_ids) do
    start_server(system, cluster_name, %{id: server_id}, machine, server_ids)
  end

  def restart_server(system, server_id) when is_atom(system) do
    case RaftEx.ServerSupSupervisor.restart_server(system, server_id, %{}) do
      {:ok, _} -> :ok
      {:ok, _, _} -> :ok
      {:error, _} = err -> err
      err -> {:error, err}
    end
  end

  def stop_server(system, server_id) when is_atom(system) do
    RaftEx.ServerSupSupervisor.stop_server(system, server_id)
  end

  def force_delete_server(system, server_id) do
    RaftEx.ServerSupSupervisor.delete_server(system, server_id)
  end

  # ---- membership changes ------------------------------------------

  def add_member(server_loc, server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.command(server_loc, {:"$ra_join", server_id, :after_log_append}, timeout)
  end

  def remove_member(server_ref, server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.command(server_ref, {:"$ra_leave", server_id, :after_log_append}, timeout)
  end

  def leave_and_terminate(system, server_ref, server_id, timeout \\ @default_timeout) do
    leave_cmd = {:"$ra_leave", server_id, :await_consensus}

    case RaftEx.ServerProc.command(server_ref, leave_cmd, timeout) do
      {:timeout, who} ->
        Logger.error("Failed to leave: request to #{inspect(who)} timed out")
        :timeout

      {:error, :noproc} = err ->
        err

      {:ok, _, _} ->
        Logger.info("RaftEx node #{inspect(server_id)} left cluster. Terminating.")
        stop_server(system, server_id)
    end
  end

  def leave_and_delete_server(system, server_ref, server_id, timeout \\ @default_timeout) do
    leave_cmd = {:"$ra_leave", server_id, :await_consensus}

    case RaftEx.ServerProc.command(server_ref, leave_cmd, timeout) do
      {:timeout, who} ->
        Logger.error("Failed to leave: request to #{inspect(who)} timed out")
        :timeout

      {:error, _} = err ->
        err

      {:ok, _, _} ->
        Logger.info("RaftEx node #{inspect(server_id)} left cluster.")
        force_delete_server(system, server_id)
    end
  end

  def trigger_election(server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.trigger_election(server_id, timeout)
  end

  def transfer_leadership(server_id, target, timeout \\ @default_timeout) do
    RaftEx.ServerProc.transfer_leadership(server_id, target, timeout)
  end

  # ---- commands ----------------------------------------------------

  def process_command(server_id, command, timeout_or_opts \\ @default_timeout)

  def process_command(server_id, command, timeout)
      when is_integer(timeout) or timeout == :infinity do
    process_command(server_id, command, %{timeout: timeout})
  end

  def process_command(server_id, command, opts) when is_map(opts) do
    timeout = Map.get(opts, :timeout, @default_timeout)

    reply_mode =
      case opts do
        %{reply_from: rf} -> {:await_consensus, %{reply_from: rf}}
        _ -> :await_consensus
      end

    RaftEx.ServerProc.command(server_id, usr(command, reply_mode), timeout)
  end

  def pipeline_command(server_id, command, correlation \\ :no_correlation, priority \\ :low)

  def pipeline_command(server_id, command, correlation, _priority)
      when correlation != :no_correlation do
    cmd = usr(command, {:notify, correlation, self()})
    RaftEx.ServerProc.cast_command(server_id, cmd)
  end

  def pipeline_command(server_id, command, :no_correlation, _priority) do
    cmd = usr(command, :noreply)
    RaftEx.ServerProc.cast_command(server_id, cmd)
  end

  # ---- queries -----------------------------------------------------

  def local_query(server_id, query_fun, timeout_or_opts \\ @default_timeout)

  def local_query(server_id, query_fun, timeout)
      when is_integer(timeout) or timeout == :infinity do
    RaftEx.ServerProc.query(server_id, query_fun, :local, %{}, timeout)
  end

  def local_query(server_id, query_fun, opts) when is_map(opts) do
    timeout = Map.get(opts, :timeout, @default_timeout)
    opts1 = Map.delete(opts, :timeout)
    RaftEx.ServerProc.query(server_id, query_fun, :local, opts1, timeout)
  end

  def leader_query(server_id, query_mfa, timeout_or_opts \\ @default_timeout)

  def leader_query(server_id, query_mfa, timeout)
      when is_integer(timeout) or timeout == :infinity do
    RaftEx.ServerProc.query(server_id, query_mfa, :leader, %{}, timeout)
  end

  def leader_query(server_id, query_mfa, opts) when is_map(opts) do
    timeout = Map.get(opts, :timeout, @default_timeout)
    opts1 = Map.delete(opts, :timeout)
    RaftEx.ServerProc.query(server_id, query_mfa, :leader, opts1, timeout)
  end

  def consistent_query(server_id, query_mfa, timeout \\ @default_timeout) do
    RaftEx.ServerProc.query(server_id, query_mfa, :consistent, %{}, timeout)
  end

  def consistent_aux(server_id, aux_cmd, timeout \\ @default_timeout) do
    RaftEx.ServerProc.query(server_id, aux_cmd, :consistent_aux, %{}, timeout)
  end

  # ---- cluster info ------------------------------------------------

  def members(server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.state_query(server_id, :members, timeout)
  end

  def members_info(server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.state_query(server_id, :members_info, timeout)
  end

  def initial_members(server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.state_query(server_id, :initial_members, timeout)
  end

  def member_overview(server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.local_state_query(server_id, :overview, timeout)
  end

  def key_metrics(server_id, timeout \\ @default_timeout)

  def key_metrics({name, n} = server_id, _timeout)
      when n == node() do
    fields = [
      :last_applied,
      :commit_index,
      :snapshot_index,
      :last_written_index,
      :last_index,
      :commit_latency,
      :term
    ]

    counters =
      case RaftEx.Counters.counters(server_id, fields) do
        nil -> %{}
        c -> c
      end

    case Process.whereis(name) do
      nil ->
        Map.merge(counters, %{state: :noproc, membership: :unknown})

      _ ->
        case :ets.lookup(:ra_state, name) do
          [] -> Map.merge(counters, %{state: :unknown, membership: :unknown})
          [{_, state, membership}] -> Map.merge(counters, %{state: state, membership: membership})
        end
    end
  end

  def key_metrics({_, n} = server_id, timeout) do
    :erpc.call(n, __MODULE__, :key_metrics, [server_id], timeout)
  end

  def overview(system) when is_atom(system) do
    case RaftEx.System.fetch(system) do
      nil ->
        :system_not_started

      %{names: %{segment_writer: seg_writer, open_mem_tbls: open_tbls, wal: _wal}} = _config ->
        %{
          node: node(),
          servers: RaftEx.Directory.overview(system),
          counters: RaftEx.Counters.overview(),
          wal: %{
            open_mem_tables: :ets.info(open_tbls, :size)
          },
          segment_writer: RaftEx.LogSegmentWriter.overview(seg_writer)
        }
    end
  end

  # ---- distributed cluster API ------------------------------------

  @doc """
  Form a distributed Raft cluster across multiple nodes.
  Delegates to `RaftEx.Cluster.form_cluster/4`.
  """
  defdelegate form_cluster(cluster_name, machine, nodes, opts \\ []), to: RaftEx.Cluster

  @doc """
  Join an existing Raft cluster.
  Delegates to `RaftEx.Cluster.join_cluster/5`.
  """
  defdelegate join_cluster(cluster_name, machine, nodes, seed_node, opts \\ []),
    to: RaftEx.Cluster

  @doc """
  Get cluster status.
  Delegates to `RaftEx.Cluster.status/1`.
  """
  defdelegate cluster_status(system \\ :default), to: RaftEx.Cluster, as: :status

  @doc """
  Start node discovery for automatic topology management.
  """
  def start_node_discovery(opts) do
    RaftEx.NodeDiscovery.start_link(opts)
  end

  @doc """
  Look up the current leader for a cluster from the local Leaderboard.
  """
  defdelegate find_leader(cluster_name), to: RaftEx.Cluster

  @doc """
  Get the local server ID for a cluster.
  """
  defdelegate local_server_id(cluster_name), to: RaftEx.Leaderboard

  @doc """
  Get overview of all clusters from the Leaderboard.
  """
  defdelegate leaderboard_overview, to: RaftEx.Leaderboard, as: :overview

  @doc """
  Start distributed Erlang and connect to seed nodes.
  """
  defdelegate start_distribution(opts \\ []), to: RaftEx.Distribution, as: :start

  # ---- aux ---------------------------------------------------------

  def aux_command(server_ref, cmd, timeout \\ 5_000) do
    :gen_statem.call(server_ref, {:aux_command, cmd}, timeout)
  end

  def cast_aux_command(server_ref, cmd) do
    :gen_statem.cast(server_ref, {:aux_command, cmd})
  end

  def trigger_compaction(server_ref) do
    :gen_statem.cast(server_ref, {:ra_log_event, :major_compaction})
  end

  def ping(server_id, timeout \\ @default_timeout) do
    RaftEx.ServerProc.ping(server_id, timeout)
  end

  # ---- uid ---------------------------------------------------------

  def new_uid(source) when is_binary(source) do
    prefix = RaftEx.Lib.derive_safe_string(source, 6)
    RaftEx.Lib.make_uid(String.upcase(prefix))
  end

  # ---- internal ----------------------------------------------------

  defp usr(data, mode), do: {:"$usr", data, mode}
end
