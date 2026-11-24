defmodule RaftEx.OTPApplication do
  @moduledoc false
  use Application

  @impl Application
  def start(_type, _args) do
    RaftEx.Infra.Supervisor.Root.start_link([])
  end

  @impl Application
  def stop(_state), do: :ok
end
