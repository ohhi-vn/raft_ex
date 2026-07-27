defmodule RaftEx.ServerSupSupervisor do
  @moduledoc """
  Dynamic supervisor that owns one `RaftEx.ServerSupervisor` per server that
  has been started on this node.

  All RPC-style functions that operate on remote nodes delegate to their
  `_rpc` counterparts via `:rpc.call/4`.
  """

  use DynamicSupervisor
  require Logger

  # Configuration keys that may be overridden when restarting an existing server.
  @mutable_config_keys [
    :install_snap_rpc_timeout,
    :await_condition_timeout,
    :max_pipeline_count,
    :ra_event_formatter,
    :min_recovery_checkpoint_interval
  ]

  # ---------------------------------------------------------------------------
  # Supervisor boilerplate
  # ---------------------------------------------------------------------------

  def start_link(%{names: %{server_sup: name}} = cfg) do
    DynamicSupervisor.start_link(__MODULE__, cfg, name: name)
  end

  @impl DynamicSupervisor
  def init(_cfg), do: DynamicSupervisor.init(strategy: :one_for_one)

  # ---------------------------------------------------------------------------
  # Server lifecycle
  # ---------------------------------------------------------------------------

  @doc "Start a new server on the node specified by `config.id`."
  @spec start_server(atom(), map()) :: DynamicSupervisor.on_start_child()
  def start_server(system, %{id: node_id, uid: uid} = config) when is_atom(system) do
    node = RaftEx.Lib.ra_server_id_node(node_id)
    :rpc.call(node, __MODULE__, :start_server_rpc, [system, uid, config])
  end

  @doc false
  def start_server_rpc(system, uid, config0) do
    case RaftEx.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      sys_cfg ->
        config = config0 |> Map.put(:system_config, sys_cfg) |> Map.put(:system, system)
        {:ok, sup_name} = RaftEx.System.lookup_name(system, :server_sup)

        spec = %{
          id:    uid,
          start: {RaftEx.ServerSupervisor, :start_link, [config]},
          type:  :supervisor
        }

        DynamicSupervisor.start_child(sup_name, spec)
    end
  end

  @doc "Restart a previously started server, merging in `add_config` overrides."
  @spec restart_server(atom(), RaftEx.Types.server_id(), map()) ::
          DynamicSupervisor.on_start_child() | {:error, term()}
  def restart_server(system, {ra_name, node}, add_config) when is_atom(system) do
    :rpc.call(node, __MODULE__, :restart_server_rpc, [system, {ra_name, node}, add_config])
  end

  @doc false
  def restart_server_rpc(system, {ra_name, _node}, add_config) when is_atom(system) do
    case RaftEx.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      sys_cfg ->
        with {:ok, config0} <- recover_config(system, ra_name) do
          {:ok, sup_name} = RaftEx.System.lookup_name(system, :server_sup)

          config =
            config0
            |> Map.merge(Map.take(add_config, @mutable_config_keys))
            |> Map.put(:system_config, sys_cfg)

          spec = %{
            id:    config.uid,
            start: {RaftEx.ServerSupervisor, :start_link, [config]},
            type:  :supervisor
          }

          DynamicSupervisor.start_child(sup_name, spec)
        end
    end
  end

  @doc "Stop a running server (does **not** delete its data)."
  @spec stop_server(atom(), RaftEx.Types.server_id()) :: :ok | {:error, term()}
  def stop_server(system, server_id) when is_atom(system) do
    node    = RaftEx.Lib.ra_server_id_node(server_id)
    ra_name = RaftEx.Lib.ra_server_id_to_local_name(server_id)

    case :rpc.call(node, __MODULE__, :prepare_server_stop_rpc, [system, ra_name]) do
      {:ok, nil, _}        -> :ok
      {:ok, pid, srv_sup}  -> Supervisor.terminate_child({srv_sup, node}, pid)
      err                  -> {:error, err}
    end
  end

  @doc false
  def prepare_server_stop_rpc(system, ra_name) do
    case RaftEx.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      %{names: %{server_sup: srv_sup} = names} ->
        parent = RaftEx.Directory.where_is_parent(names, ra_name)
        {:ok, parent, srv_sup}
    end
  end

  @doc "Stop and permanently delete a server, removing its data directory."
  @spec delete_server(atom(), RaftEx.Types.server_id()) :: :ok | {:error, term()}
  def delete_server(system, server_id) when is_atom(system) do
    node = RaftEx.Lib.ra_server_id_node(server_id)
    name = RaftEx.Lib.ra_server_id_to_local_name(server_id)

    with :ok <- stop_server(system, server_id) do
      :rpc.call(node, __MODULE__, :delete_server_rpc, [system, name])
    end
  end

  @doc false
  def delete_server_rpc(system, ra_name) do
    case RaftEx.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      %{names: %{log_meta: meta, server_sup: srv_sup} = names} ->
        Logger.info("Deleting server #{ra_name} and its data directory.")

        uid = RaftEx.Directory.uid_of(names, ra_name)
        RaftEx.LogMeta.delete(meta, uid)

        dir = RaftEx.Env.server_data_dir(system, uid)
        DynamicSupervisor.terminate_child(srv_sup, uid)
        RaftEx.Lib.recursive_delete(dir)
        RaftEx.Directory.unregister_name(names, uid)
        RaftEx.Counters.delete({ra_name, node()})
        RaftEx.Leaderboard.clear(ra_name)
        :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  @doc false
  @spec recover_config(atom(), atom()) :: {:ok, map()} | {:error, term()}
  def recover_config(system, ra_name) do
    case RaftEx.Directory.uid_of(system, ra_name) do
      nil -> {:error, :name_not_registered}
      uid -> RaftEx.Log.read_config(RaftEx.Env.server_data_dir(system, uid))
    end
  end

  @doc "Keys that callers are permitted to override when restarting a server."
  @spec mutable_config_keys() :: [atom()]
  def mutable_config_keys, do: @mutable_config_keys
end
