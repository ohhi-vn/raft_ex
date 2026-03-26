defmodule RaftEx.SystemRecover do
  use GenServer
  require Logger

  def start_link(system) when is_atom(system) do
    GenServer.start_link(__MODULE__, system, [])
  end

  @impl GenServer
  def init(system) do
    conf = RaftEx.System.fetch(system)

    case conf do
      %{server_recovery_strategy: :registered} ->
        regd = RaftEx.Directory.list_registered(system)

        Logger.info(
          "#{__MODULE__}: system '#{system}' recovery strategy :registered, #{length(regd)} servers"
        )

        Enum.each(regd, fn {n, _uid} ->
          case RaftEx.restart_server(system, {n, node()}) do
            :ok -> :ok
            err -> Logger.warning("#{__MODULE__}: restart_server failed with #{inspect(err)}")
          end
        end)

      %{server_recovery_strategy: {mod, fun, args}} ->
        Logger.info("#{__MODULE__}: system '#{system}' recovery via #{mod}.#{fun}")
        apply(mod, fun, [system | args])

      _ ->
        Logger.debug("#{__MODULE__}: no server recovery configured")
    end

    {:ok, %{}, :hibernate}
  end
end
