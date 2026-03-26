defmodule RaftEx.Worker do
  use GenServer
  require Logger

  def start_link(%{id: _id} = config) do
    GenServer.start_link(__MODULE__, config, hibernate_after: 30_000)
  end

  def queue_work(pid, fun_or_mfa, err_fun) when is_pid(pid) do
    GenServer.cast(pid, {:work, fun_or_mfa, err_fun})
  end

  @impl GenServer
  def init(%{id: id} = config) do
    Process.flag(:trap_exit, true)
    log_id = Map.get(config, :friendly_name, inspect(id))
    {:ok, %{log_id: log_id}}
  end

  @impl GenServer
  def handle_cast({:work, fun_or_mfa, err_fun}, state) do
    try do
      case fun_or_mfa do
        {m, f, args} -> apply(m, f, args)
        fun when is_function(fun) -> fun.()
      end
    rescue
      e ->
        Logger.warning("#{state.log_id}: worker error #{inspect(e)}")
        err_fun.({:error, e})
    end

    :erlang.garbage_collect()
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_, state), do: {:noreply, state}
end
