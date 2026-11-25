defmodule RaftEx.OTPApplication do
  @moduledoc false
  use Application

  alias RaftEx.Infra.Supervisor.Root

  @impl Application
  def start(_type, _args) do
    Root.start_link([])
  end

  @impl Application
  def stop(_state), do: :ok
end
