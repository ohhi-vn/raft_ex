defmodule RaftEx.LogWal do
  @moduledoc """
  Write-ahead log process.  Accepts batched append requests from server
  processes and writes them durably before acknowledging.

  This is a stub implementation.  The full version mirrors `ra_log_wal.erl`.
  """

  use GenServer
  require Logger

  def start_link(%{names: %{wal: name}} = config) do
    GenServer.start_link(__MODULE__, config, name: name)
  end

  @impl GenServer
  def init(config) do
    Logger.debug("RaftEx.LogWal started")
    {:ok, %{config: config, entries: []}}
  end

  @impl GenServer
  def handle_cast({:write, _entries} = msg, state) do
    # Full impl: batch writes, fsync, then notify senders.
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}
end
