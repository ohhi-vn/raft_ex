defmodule RaftEx.Infra.Supervisor.ServerSupSupervisor do
  @moduledoc """
  Dynamic supervisor that owns one `RaftEx.Infra.Supervisor.ServerSupervisor` per server that
  has been started on this node.

  All RPC-style functions that operate on remote nodes delegate to their
  `_rpc` counterparts via `:rpc.call/4`.
  """

  use DynamicSupervisor
  require Logger

  alias RaftEx.Domain.Utils
  alias RaftEx.Infra.Config.Env
  alias RaftEx.Infra.Filesystem
  alias RaftEx.Infra.Observability.Counters
  alias RaftEx.Infra.Persistence.Log
  alias RaftEx.Infra.Persistence.LogMeta
  alias RaftEx.Infra.Registry.Directory

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
    node = Utils.ra_server_id_node(node_id)
    :rpc.call(node, __MODULE__, :start_server_rpc, [system, uid, config])
  end

  @doc false
  def start_server_rpc(system, uid, config0) do
    case RaftEx.App.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      sys_cfg ->
        config = config0 |> Map.put(:system_config, sys_cfg) |> Map.put(:system, system)
        {:ok, sup_name} = RaftEx.App.System.lookup_name(system, :server_sup)

        spec = %{
          id: uid,
          start: {RaftEx.Infra.Supervisor.ServerSupervisor, :start_link, [config]},
          type: :supervisor
        }

        result = DynamicSupervisor.start_child(sup_name, spec)

        if match?({:ok, _}, result) or match?({:ok, _, _}, result) do
          Counters.new(config.id, Counters.counter_fields())
        end

        result
    end
  end

  @doc "Restart a previously started server, merging in `add_config` overrides."
  @spec restart_server(atom(), RaftEx.Domain.Types.server_id(), map()) ::
          DynamicSupervisor.on_start_child() | {:error, term()}
  def restart_server(system, {ra_name, node}, add_config) when is_atom(system) do
    :rpc.call(node, __MODULE__, :restart_server_rpc, [system, {ra_name, node}, add_config])
  end

  @doc false
  def restart_server_rpc(system, {ra_name, _node}, add_config) when is_atom(system) do
    case RaftEx.App.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      sys_cfg ->
        with {:ok, config0} <- recover_config(system, ra_name) do
          {:ok, sup_name} = RaftEx.App.System.lookup_name(system, :server_sup)

          config =
            config0
            |> Map.merge(Map.take(add_config, @mutable_config_keys))
            |> Map.put(:system_config, sys_cfg)

          spec = %{
            id: config.uid,
            start: {RaftEx.Infra.Supervisor.ServerSupervisor, :start_link, [config]},
            type: :supervisor
          }

          result = DynamicSupervisor.start_child(sup_name, spec)
          register_server_counters(result, config)
          result
        end
    end
  end

  @doc "Stop a running server (does **not** delete its data)."
  @spec stop_server(atom(), RaftEx.Domain.Types.server_id()) :: :ok | {:error, term()}
  def stop_server(system, server_id) when is_atom(system) do
    node = Utils.ra_server_id_node(server_id)
    ra_name = Utils.ra_server_id_to_local_name(server_id)

    case :rpc.call(node, __MODULE__, :stop_server_rpc, [system, ra_name]) do
      :ok -> :ok
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end

  @doc false
  def stop_server_rpc(system, ra_name) do
    case RaftEx.App.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      %{names: %{server_sup: srv_sup}} ->
        result =
          case find_child(srv_sup, ra_name) do
            nil -> :ok
            pid -> DynamicSupervisor.terminate_child(srv_sup, pid)
          end

        RaftEx.Leaderboard.forget_leader({ra_name, node()})
        result
    end
  end

  @doc false
  def prepare_server_stop_rpc(system, ra_name) do
    case RaftEx.App.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      %{names: %{server_sup: srv_sup}} ->
        {:ok, find_child(srv_sup, ra_name), srv_sup}
    end
  end

  # `DynamicSupervisor` children all have the id `:undefined`; the server name
  # is the id of the runtime child inside each `ServerSupervisor`. Walk one
  # level down to find the supervisor that owns `ra_name`.
  defp register_server_counters(result, config) do
    if match?({:ok, _}, result) or match?({:ok, _, _}, result) do
      Counters.new(config.id, Counters.counter_fields())
    end
  end

  defp find_child(srv_sup, ra_name) do
    srv_sup
    |> DynamicSupervisor.which_children()
    |> Enum.find_value(fn
      {_id, pid, :supervisor, _modules} when is_pid(pid) ->
        try do
          case Supervisor.which_children(pid) do
            [{^ra_name, _child, _type, _modules}] -> pid
            _ -> nil
          end
        catch
          :exit, _ -> nil
        end

      _ ->
        nil
    end)
  end

  @doc "Stop and permanently delete a server, removing its data directory."
  @spec delete_server(atom(), RaftEx.Domain.Types.server_id()) :: :ok | {:error, term()}
  def delete_server(system, server_id) when is_atom(system) do
    node = Utils.ra_server_id_node(server_id)
    name = Utils.ra_server_id_to_local_name(server_id)

    with :ok <- stop_server(system, server_id) do
      :rpc.call(node, __MODULE__, :delete_server_rpc, [system, name])
    end
  end

  @doc false
  def delete_server_rpc(system, ra_name) do
    case RaftEx.App.System.fetch(system) do
      nil ->
        {:error, :system_not_started}

      %{names: %{log_meta: meta, server_sup: srv_sup} = names} ->
        Logger.info("Deleting server #{ra_name} and its data directory.")

        uid = Directory.uid_of(names, ra_name)
        LogMeta.delete(meta, uid)

        dir = Env.server_data_dir(system, uid)

        case find_child(srv_sup, ra_name) do
          nil -> :ok
          pid -> DynamicSupervisor.terminate_child(srv_sup, pid)
        end

        Filesystem.recursive_delete(dir)
        Directory.unregister_name(names, uid)
        Counters.delete({ra_name, node()})
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
    case Directory.uid_of(system, ra_name) do
      nil ->
        {:error, :name_not_registered}

      uid ->
        Log.read_config(Env.server_data_dir(system, uid))
    end
  end

  @doc "Keys that callers are permitted to override when restarting a server."
  @spec mutable_config_keys() :: [atom()]
  def mutable_config_keys, do: @mutable_config_keys
end
