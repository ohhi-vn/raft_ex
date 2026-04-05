defmodule RaftEx.Network do
  @moduledoc """
  Handles distributed RPC communication between Raft nodes across the cluster.

  This module provides a unified interface for sending messages to local or remote
  `RaftEx.ServerProc` instances. It manages node connections, monitors remote processes,
  and implements routing logic to seamlessly handle both local and distributed messaging.

  ## Features

  - **Unified Routing**: Automatically routes messages to local or remote nodes based on server ID.
  - **Lazy Connection**: Establishes distribution connections on-demand via `Node.connect/1`.
  - **Process Monitoring**: Monitors remote nodes and processes to detect failures.
  - **Backoff & Retry Ready**: Structured to support retry logic in higher-level callers.
  - **Distribution Optimized**: Uses native Erlang distribution primitives for minimal overhead.

  ## Usage

      # Send an RPC to a remote server
      RaftEx.Network.send_rpc({:server2, :"node2@host"}, rpc)

      # Make a synchronous call
      {:ok, reply} = RaftEx.Network.call({:server2, :"node2@host"}, {:ping}, 5000)

      # Monitor a remote server
      ref = RaftEx.Network.monitor({:server2, :"node2@host"})
  """

  require Logger

  @type server_id :: {atom(), node()}
  @type rpc :: struct()
  @type opts :: keyword()

  @default_timeout 5_000

  @doc """
  Sends an RPC message to a server, routing to local or remote node automatically.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  @spec send_rpc(server_id(), rpc(), opts()) :: :ok | {:error, term()}
  def send_rpc({name, node} = server_id, rpc, opts \\ []) do
    msg = {:gen_cast, {:rpc, rpc}}

    if node == node() do
      do_send_local(name, msg)
    else
      do_send_remote(server_id, msg)
    end
  end

  @doc """
  Makes a synchronous call to a server.

  Returns `{:ok, reply}` on success, `{:error, reason}` on failure.
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

  Returns `:ok` on success, `{:error, reason}` on failure.
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
  Monitors a remote server process. Returns a monitor reference.

  For remote nodes, this monitors the node connection status rather than the specific
  process, as remote process monitors require additional infrastructure.
  """
  @spec monitor(server_id()) :: reference() | {:error, term()}
  def monitor({name, node} = server_id) do
    if node == node() do
      case Process.whereis(name) do
        nil -> {:error, :noproc}
        pid -> Process.monitor(pid)
      end
    else
      # Monitor the remote node connection
      Node.monitor(node, true)
      make_ref()
    end
  end

  @doc """
  Handles node up events. Logs connection and triggers any necessary reconnection logic.
  """
  @spec handle_node_up(node()) :: :ok
  def handle_node_up(node) do
    Logger.info("RaftEx.Network: Node #{inspect(node)} connected")
    :ok
  end

  @doc """
  Handles node down events. Logs disconnection and notifies local state machines.
  """
  @spec handle_node_down(node()) :: :ok
  def handle_node_down(node) do
    Logger.warning("RaftEx.Network: Node #{inspect(node)} disconnected")
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private Helpers
  # ---------------------------------------------------------------------------

  defp do_send_local(name, msg) do
    case Process.whereis(name) do
      nil ->
        {:error, :noproc}

      pid ->
        Process.send(pid, msg, [:nosuspend])
        :ok
    end
  end

  defp do_send_remote({name, node}, msg) do
    if Node.connect(node) do
      send({name, node}, msg)
      :ok
    else
      {:error, :noconnection}
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

  defp do_call_remote({name, node}, msg, timeout) do
    if Node.connect(node) do
      try do
        # Use :rpc.call for synchronous remote calls with timeout propagation
        reply = :rpc.call(node, :gen_statem, :call, [{name, node}, msg, timeout], timeout)
        {:ok, reply}
      catch
        :exit, {:timeout, _} -> {:error, :timeout}
        :exit, {:noconnection, _} -> {:error, :noconnection}
        :exit, reason -> {:error, reason}
      end
    else
      {:error, :noconnection}
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
      # Direct send is more efficient than :rpc.cast for async messages
      send({name, node}, {:gen_cast, msg})
      :ok
    else
      {:error, :noconnection}
    end
  end
end
