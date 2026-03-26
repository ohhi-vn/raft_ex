defmodule RaftEx.LogSegmentWriter do
  @moduledoc """
  Background process that compacts WAL entries into immutable segment files.

  This is a stub implementation.  The full version mirrors `ra_log_segment_writer.erl`
  and uses `gen_batch_server` for efficient batching.
  """

  use GenServer
  require Logger

  def start_link(%{name: name} = config) do
    GenServer.start_link(__MODULE__, config, name: name)
  end

  def overview(pid) do
    GenServer.call(pid, :overview)
  end

  @impl GenServer
  def init(config) do
    Logger.debug("RaftEx.LogSegmentWriter started for system #{config[:system]}")
    {:ok, %{config: config, segments: []}}
  end

  @impl GenServer
  def handle_call(:overview, _from, state) do
    {:reply, %{num_segments: length(state.segments)}, state}
  end

  @impl GenServer
  def handle_cast(_msg, state), do: {:noreply, state}

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}
end
