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

  Never blocks the caller: remote sends use `:nosuspend`/`:noconnect` and
  fail fast with `{:error, reason}` when the peer is unreachable, so a
  partitioned peer can never delay replication to reachable peers. Callers
  retry on their own schedule (tick, acknowledgement-triggered pipeline, or
  node-up reconciliation) instead of sleeping inline.

  Pass `sender: server_id` in `opts` to attribute the message without a
  remote lookup on the receiving side.
  """
  @spec send_rpc(server_id(), rpc(), opts()) :: :ok | {:error, term()}
  def send_rpc({name, node} = server_id, rpc, opts \\ []) do
    msg =
      case Keyword.get(opts, :sender) do
        {sname, snode} = sender when is_atom(sname) and is_atom(snode) ->
          {:gen_cast, {:rpc, sender, rpc}}

        _ ->
          {:gen_cast, {:rpc, rpc}}
      end

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
  Casts a message to a server asynchronously. Never blocks: a partitioned
  peer yields `{:error, reason}` immediately instead of stalling the caller.
  """
  @spec cast(server_id(), term()) :: :ok | {:error, term()}
  def cast({name, node} = server_id, msg) do
    if node == node() do
      do_cast_local(name, msg)
    else
      do_send_remote(server_id, msg)
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

  defp do_send_remote({name, node} = server_id, msg) do
    # Non-blocking: `:nosuspend` means a partitioned peer cannot stall the
    # caller; failures surface as `{:error, reason}` for the caller to
    # handle (rollback, suspect marking, scheduled retry) instead of
    # sleeping inline. The outer tuple preserves pid attribution for legacy
    # receivers; the `{:rpc, sender, ...}` envelope inside `msg` carries
    # the server id.
    :erlang.send({name, node}, {self(), msg}, [:nosuspend])
    :ok
  catch
    _, :noconnection ->
      {:error, :noconnection}

    _, reason ->
      Logger.debug(
        "RaftEx.Infra.Network.Transport: send to #{inspect(server_id)} failed: #{inspect(reason)}"
      )

      {:error, reason}
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
    # Single attempt, no inline sleeps: callers decide their own retry
    # schedule. A failed call returns an error immediately.
    try do
      reply = :rpc.call(node, :gen_statem, :call, [{name, node}, msg, timeout], timeout)
      {:ok, reply}
    catch
      :exit, {:timeout, _} ->
        {:error, :timeout}

      :exit, {:noconnection, _} ->
        {:error, :noconnection}

      :exit, reason ->
        {:error, reason}
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
