defmodule RaftEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :ra,
      version: "2.0.0",
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
