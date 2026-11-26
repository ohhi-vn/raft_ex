defmodule RaftEx.Infra.Network.Transport do
  @moduledoc """
  Handles distributed RPC communication between Raft nodes across the cluster.
  """

  require Logger

  alias RaftEx.Infra.Network.Distribution

  @type server_id :: {atom(), node()}
  @type rpc :: struct()
  @type opts :: keyword()

  @default_timeout 5_000

  @doc """
  Start node monitoring for Raft cluster communication.
  Sets up monitoring of remote nodes and connects to seed nodes.

  ## Options
    * `:seeds` - list of seed nodes to connect to
    * `:cookie` - Erlang distribution cookie
  """
  def start_monitoring(opts \\ []) do
    seeds = Keyword.get(opts, :seeds, [])
    cookie = Keyword.get(opts, :cookie, :raft_ex)

    connect_to_seeds(seeds, cookie)

    Node.monitor(:all, true)
    :ok
  end

  defp connect_to_seeds([], _cookie), do: :ok

  defp connect_to_seeds(seeds, cookie) do
    Distribution.set_cookie(cookie)

    seeds
    |> Enum.reject(&(&1 == node()))
    |> Enum.each(&log_connect_result/1)
  end

  defp log_connect_result(seed) do
    case Node.connect(seed) do
      true ->
        Logger.info("RaftEx.Infra.Network.Transport: connected to seed #{inspect(seed)}")

      false ->
        Logger.warning(
          "RaftEx.Infra.Network.Transport: failed to connect to seed #{inspect(seed)}"
        )
    end
  end

  @doc """
  Sends an RPC message to a server.
  """
  @spec send_rpc(server_id(), rpc(), opts()) :: :ok | {:error, term()}
  def send_rpc({name, node} = server_id, rpc, _opts \\ []) do
    msg = {:gen_cast, {:rpc, rpc}}

    if node == node() do
      do_send_local(name, msg)
    else
      do_send_remote(server_id, msg)
    end
  end

  @doc """
  Makes a synchronous call to a server.
  """
  @spec call(server_id(), term(), timeout()) :: {:ok, term()} | {:error, term()}
  def call({name, node} = server_id, msg, timeout \\ @default_timeout) do
    if node == node() do
      do_call_local(name, msg, timeout)
    else
      do_call_remote(server_id, msg, timeout)
    end
  end

  @doc """
  Casts a message to a server asynchronously.
  """
  @spec cast(server_id(), term()) :: :ok | {:error, term()}
  def cast({name, node} = server_id, msg) do
    if node == node() do
      do_cast_local(name, msg)
    else
      do_cast_remote(server_id, msg)
    end
  end

  @doc """
  Monitors a remote server process.
  """
  @spec monitor(server_id()) :: reference() | {:error, term()}
  def monitor({name, node} = _server_id) do
    if node == node() do
      case Process.whereis(name) do
        nil -> {:error, :noproc}
        pid -> Process.monitor(pid)
      end
    else
      Node.monitor(node, true)
      make_ref()
    end
  end

  @doc """
  Handle node up events. Attempts to re-register with any Raft clusters
  the local node was part of.
  """
  @spec handle_node_up(node()) :: :ok
  def handle_node_up(node) do
    Logger.info("RaftEx.Infra.Network.Transport: node #{inspect(node)} connected")

    # Attempt to re-establish Raft connections
    notify_server_procs(node, :nodeup)

    :ok
  end

  @doc """
  Handle node down events.
  """
  @spec handle_node_down(node()) :: :ok
  def handle_node_down(node) do
    Logger.warning("RaftEx.Infra.Network.Transport: node #{inspect(node)} disconnected")

    notify_server_procs(node, :nodedown)

    :ok
  end

  defp do_send_local(name, msg) do
    case Process.whereis(name) do
      nil ->
        {:error, :noproc}

      pid ->
        Process.send(pid, msg, [:nosuspend])
        :ok
    end
  end

  defp do_send_remote(server_id, msg) do
    do_send_remote(server_id, msg, 3)
  end

  defp do_send_remote(server_id, _msg, 0) do
    Logger.warning(
      "RaftEx.Infra.Network.Transport: failed to send to #{inspect(server_id)} after retries"
    )

    {:error, :noconnection}
  end

  defp do_send_remote({name, node} = server_id, msg, attempts) do
    if Node.connect(node) do
      send({name, node}, {self(), msg})
      :ok
    else
      Logger.warning(
        "RaftEx.Infra.Network.Transport: no connection to #{inspect(node)} (#{attempts} attempts left)"
      )

      Process.sleep(100)
      do_send_remote(server_id, msg, attempts - 1)
    end
  end

  defp do_call_local(name, msg, timeout) do
    case Process.whereis(name) do
      nil ->
        {:error, :noproc}

      pid ->
        try do
          reply = :gen_statem.call(pid, msg, timeout)
          {:ok, reply}
        catch
          :exit, {:timeout, _} -> {:error, :timeout}
          :exit, reason -> {:error, reason}
        end
    end
  end

  defp do_call_remote(server_id, msg, timeout) do
    do_call_remote(server_id, msg, timeout, 3)
  end

  defp do_call_remote(server_id, _msg, _timeout, 0) do
    Logger.warning(
      "RaftEx.Infra.Network.Transport: call failed to #{inspect(server_id)} after retries"
    )

    {:error, :noconnection}
  end

  defp do_call_remote({name, node} = server_id, msg, timeout, attempts) do
    if Node.connect(node) do
      try do
        reply = :rpc.call(node, :gen_statem, :call, [{name, node}, msg, timeout], timeout)
        {:ok, reply}
      catch
        :exit, {:timeout, _} ->
          {:error, :timeout}

        :exit, {:noconnection, _} ->
          Logger.warning(
            "RaftEx.Infra.Network.Transport: no connection to #{inspect(node)} (#{attempts} attempts left)"
          )

          Process.sleep(100)
          do_call_remote(server_id, msg, timeout, attempts - 1)

        :exit, reason ->
          {:error, reason}
      end
    else
      Logger.warning(
        "RaftEx.Infra.Network.Transport: no connection to #{inspect(node)} (#{attempts} attempts left)"
      )

      Process.sleep(100)
      do_call_remote(server_id, msg, timeout, attempts - 1)
    end
  end

  defp do_cast_local(name, msg) do
    case Process.whereis(name) do
      nil ->
        {:error, :noproc}

      pid ->
        :gen_statem.cast(pid, msg)
        :ok
    end
  end

  defp do_cast_remote({name, node}, msg) do
    if Node.connect(node) do
      send({name, node}, {:gen_cast, msg})
      :ok
    else
      {:error, :noconnection}
    end
  end

  defp notify_server_procs(node, event) do
    servers = matched_server_procs()

    for [name, _state, _membership] <- servers do
      case Process.whereis(name) do
        nil ->
          :ok

        pid ->
          Process.send(pid, {:ra_network_event, event, node}, [:nosuspend])
      end
    end
  end

  defp matched_server_procs do
    :ets.match(:ra_state, {:"$1", :"$2", :"$3"})
  rescue
    _ -> []
  end
end
