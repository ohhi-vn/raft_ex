defmodule RaftEx.MixProject do
  use Mix.Project

  @source_url "https://github.com/manhvu/raft_ex"
  @version "0.0.1"

  def project do
    [
      app: :raft_ex,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto], mod: {RaftEx.Application, []}]
  end

  defp deps do
    [
      {:seshat, "~> 0.6"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp description do
    "⚠️ EXPERIMENTAL - NOT FOR PRODUCTION ⚠️ Elixir port of RabbitMQ RA Raft consensus. Implements leader election, log replication, snapshots, and cluster management. APIs are unstable and under active development."
  end

  defp package do
    [
      maintainers: ["Maintainer Name"],
      licenses: ["MPL-2.0", "Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: ~w(lib mix.exs README.md LICENSE LICENSE-APACHE2 LICENSE-MPL-RabbitMQ)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end
end
