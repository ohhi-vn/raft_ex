defmodule RaftEx.MixProject do
  use Mix.Project

  @source_url "https://github.com/ohhi-vn/raft_ex"
  @version "0.7.0"

  def project do
    [
      app: :raft_ex,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      test_coverage: [tool: ExCoveralls],
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit],
        plt_core_path: "priv/plts",
        plt_local_path: "priv/plts",
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        flags: [
          :error_handling,
          :underspecs,
          :unmatched_returns,
          :unknown
        ],
        ignore_warnings: ".dialyzer_ignore.exs"
      ],
      aliases: aliases()
    ]
  end

  def cli do
    [
      preferred_envs: [
        precommit: :test,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/fixtures", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [extra_applications: [:logger, :crypto], mod: {RaftEx.OTPApplication, []}]
  end

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},

      # Test dependencies
      {:excoveralls, "~> 0.18", only: :test},

      # Code quality
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4.5", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    "Elixir implement of Raft consensus. Implements leader election, log replication, snapshots, and cluster management."
  end

  defp package do
    [
      maintainers: ["Maintainer Name"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: ~w(lib mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  defp docs do
    guides = [
      "guides/getting-started.md",
      "guides/architecture.md",
      "guides/state-machine.md",
      "guides/persistence.md",
      "guides/membership.md",
      "guides/consistent-queries.md",
      "guides/leadership-transfer.md",
      "guides/observability.md",
      "guides/configuration.md",
      "guides/troubleshooting.md"
    ]

    [
      main: "readme",
      extras:
        [
          "README.md",
          "CHANGELOG.md",
          "docs/metrics.md",
          "docs/persistence-invariants.md",
          "docs/quality-baseline.md"
        ] ++ guides,
      groups_for_extras: [
        Guides: guides
      ],
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end

  defp aliases do
    [
      precommit: [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "test"
      ],
      quality: ["format", "credo --strict", "dialyzer"],
      "quality.ci": [
        "format --check-formatted",
        "credo --strict",
        "dialyzer",
        "docs --warnings-as-errors"
      ],
      "release.check": ["cmd bash scripts/release_check.sh"]
    ]
  end
end
