defmodule RaftEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :raft_ex,
      version: "0.0.1",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto], mod: {RaftEx.Application, []}]
  end

  defp deps do
    [{:seshat, "~> 0.6"}]
  end
end
