defmodule RaftEx.Application do
  @moduledoc false
  use Application

  @impl Application
  def start(_type, _args) do
    RaftEx.Supervisor.start_link([])
  end

  @impl Application
  def stop(_state), do: :ok
end
