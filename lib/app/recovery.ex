defmodule RaftEx.App.Recovery do
  @moduledoc """
  Restores registered servers when a RaftEx system starts.
  """

  use GenServer
  require Logger

  alias RaftEx.Infra.Registry.Directory

  def start_link(system) when is_atom(system) do
    GenServer.start_link(__MODULE__, system, [])
  end

  @impl GenServer
  def init(system) do
    conf = RaftEx.App.System.fetch(system)

    case conf do
      %{server_recovery_strategy: :registered} ->
        regd = Directory.list_registered(system)

        Logger.info(
          "#{__MODULE__}: system '#{system}' recovery strategy :registered, #{length(regd)} servers"
        )

        restart_registered_servers(system, regd)

      %{server_recovery_strategy: {mod, fun, args}} ->
        Logger.info("#{__MODULE__}: system '#{system}' recovery via #{mod}.#{fun}")
        apply(mod, fun, [system | args])

      _ ->
        Logger.debug("#{__MODULE__}: no server recovery configured")
    end

    {:ok, %{}, :hibernate}
  end

  defp restart_registered_servers(system, regd) do
    Enum.each(regd, fn {n, _uid} ->
      case RaftEx.restart_server(system, {n, node()}) do
        :ok -> :ok
        err -> Logger.warning("#{__MODULE__}: restart_server failed with #{inspect(err)}")
      end
    end)
  end
end
