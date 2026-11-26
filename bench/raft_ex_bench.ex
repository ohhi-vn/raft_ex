defmodule RaftExBench do
  @moduledoc false

  alias RaftExBench.{JSON, Scenarios}

  @version 1
  @baseline_path "bench/baseline.json"

  @defaults %{
    scenario: nil,
    iterations: 5,
    warmup: 1,
    record: false,
    baseline: @baseline_path,
    compare: nil,
    threshold: 10.0,
    scale: 1.0,
    sync: nil,
    json: false,
    help: false
  }

  @spec main([String.t()]) :: :ok
  def main(argv) do
    opts = parse_args!(argv)

    if opts.help do
      IO.puts(help())
      System.halt(0)
    end

    report = run(opts)

    if opts.record do
      record!(report, opts.baseline)
    end

    if opts.json do
      IO.puts(JSON.encode(report))
    else
      print_table(report)
    end

    exit_code =
      if opts.compare do
        compare_and_print(report, opts)
      else
        0
      end

    if exit_code != 0 do
      System.halt(exit_code)
    end

    :ok
  end

  @spec run(map()) :: map()
  def run(opts) do
    opts = Map.merge(@defaults, opts)
    names = if opts.scenario, do: [opts.scenario], else: Scenarios.names()

    results = Enum.map(names, &measure(&1, opts))

    %{
      "version" => @version,
      "generated_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "revision" => git_revision(),
      "otp" => otp_release(),
      "elixir" => System.version(),
      "scenarios" => results
    }
  end

  defp measure(name, opts) do
    warmup = max(0, opts.warmup)
    iterations = max(1, opts.iterations)

    if warmup > 0 do
      Enum.each(1..warmup, fn _ -> Scenarios.run(name, opts) end)
    end

    runs = for _ <- 1..iterations, do: Scenarios.run(name, opts)
    total_us = runs |> Enum.map(&elem(&1, 0)) |> Enum.sum()
    input = runs |> hd() |> elem(1)
    per_iteration_us = total_us / iterations
    metric = Scenarios.metric(name)

    value =
      case metric do
        :throughput ->
          if total_us == 0 do
            0.0
          else
            iterations * input / (total_us / 1_000_000)
          end

        :latency ->
          per_iteration_us
      end

    %{
      "name" => name,
      "metric" => Atom.to_string(metric),
      "unit" => Scenarios.unit(name),
      "iterations" => iterations,
      "warmup" => warmup,
      "input" => input,
      "elapsed_us" => total_us,
      "per_iteration_us" => per_iteration_us,
      "value" => value,
      "sync" => Atom.to_string(opts.sync || Scenarios.default_sync(name))
    }
  end

  # ---------------------------------------------------------------------------
  # Reporting
  # ---------------------------------------------------------------------------

  defp print_table(report) do
    IO.puts("")
    IO.puts("RaftEx benchmark")
    IO.puts("  revision: #{report["revision"]}")
    IO.puts("  runtime:  OTP #{report["otp"]} / Elixir #{report["elixir"]}")
    IO.puts("")

    header =
      pad("scenario", 24) <>
        pad("iters", 7) <>
        pad("input", 10) <>
        pad("elapsed", 12) <>
        pad("throughput / latency", 28) <>
        "sync"

    IO.puts(header)
    IO.puts(String.duplicate("-", String.length(header)))

    Enum.each(report["scenarios"], fn scenario ->
      IO.puts(
        pad(scenario["name"], 24) <>
          pad(Integer.to_string(scenario["iterations"]), 7) <>
          pad(Integer.to_string(scenario["input"]), 10) <>
          pad(format_ms(scenario["elapsed_us"]), 12) <>
          pad(format_value(scenario), 28) <>
          scenario["sync"]
      )
    end)

    IO.puts("")
  end

  defp pad(value, width) when is_binary(value), do: String.pad_trailing(value, width)
  defp pad(value, width), do: value |> to_string() |> String.pad_trailing(width)

  defp format_ms(us), do: :erlang.float_to_binary(us / 1000, decimals: 2) <> " ms"

  defp format_value(%{"metric" => "throughput"} = scenario) do
    :erlang.float_to_binary(scenario["value"], decimals: 1) <> " " <> scenario["unit"]
  end

  defp format_value(scenario) do
    :erlang.float_to_binary(scenario["value"], decimals: 1) <> " " <> scenario["unit"]
  end

  # ---------------------------------------------------------------------------
  # Baseline
  # ---------------------------------------------------------------------------

  defp record!(report, path) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, JSON.encode(report) <> "\n")
    IO.puts("Recorded baseline to #{path}")
  end

  defp compare_and_print(report, opts) do
    baseline = opts.compare |> File.read!() |> JSON.decode()
    {rows, exit_code} = compare(report, baseline, opts.threshold)

    IO.puts("")
    IO.puts("Comparison against #{opts.compare} (threshold #{opts.threshold}%)")
    IO.puts("")

    Enum.each(rows, fn row ->
      case row["status"] do
        "new" ->
          IO.puts("  #{pad(row["name"], 24)} new (not in baseline)")

        _ ->
          IO.puts(
            "  " <>
              pad(row["name"], 24) <>
              pad(format_pct(row["change_pct"]), 12) <>
              row["status"]
          )
      end
    end)

    IO.puts("")
    exit_code
  end

  @doc "Compare a report against a decoded baseline. Returns `{rows, exit_code}`."
  @spec compare(map(), map(), number()) :: {[map()], 0 | 1}
  def compare(report, baseline, threshold) do
    baseline_by_name =
      Map.new(baseline["scenarios"], fn scenario -> {scenario["name"], scenario} end)

    rows =
      Enum.map(report["scenarios"], fn scenario ->
        case Map.fetch(baseline_by_name, scenario["name"]) do
          {:ok, old} ->
            comparison_row(scenario, old, threshold)

          :error ->
            %{
              "name" => scenario["name"],
              "status" => "new",
              "change_pct" => nil,
              "regression" => false
            }
        end
      end)

    exit_code = if Enum.any?(rows, & &1["regression"]), do: 1, else: 0
    {rows, exit_code}
  end

  defp comparison_row(scenario, old, threshold) do
    old_value = old["value"]
    new_value = scenario["value"]

    change_pct =
      if old_value == 0 do
        0.0
      else
        (new_value - old_value) / old_value * 100
      end

    regression =
      case scenario["metric"] do
        "throughput" -> change_pct < -threshold
        _ -> change_pct > threshold
      end

    %{
      "name" => scenario["name"],
      "status" => if(regression, do: "regression", else: "ok"),
      "baseline" => old_value,
      "current" => new_value,
      "change_pct" => change_pct,
      "regression" => regression
    }
  end

  defp format_pct(nil), do: "n/a"
  defp format_pct(value), do: :erlang.float_to_binary(value, decimals: 1) <> "%"

  # ---------------------------------------------------------------------------
  # Options
  # ---------------------------------------------------------------------------

  defp parse_args!(argv) do
    {parsed, rest, invalid} =
      OptionParser.parse(argv,
        strict: [
          scenario: :string,
          iterations: :integer,
          warmup: :integer,
          record: :boolean,
          baseline: :string,
          compare: :string,
          threshold: :float,
          scale: :float,
          sync: :string,
          json: :boolean,
          help: :boolean
        ]
      )

    if invalid != [] do
      raise ArgumentError, "invalid options: #{inspect(invalid)}\n\n#{help()}"
    end

    if rest != [] do
      raise ArgumentError, "unexpected arguments: #{inspect(rest)}\n\n#{help()}"
    end

    opts = Enum.reduce(parsed, @defaults, fn {key, value}, acc -> Map.put(acc, key, value) end)
    opts = %{opts | sync: parse_sync(opts.sync)}

    if opts.scenario && opts.scenario not in Scenarios.names() do
      raise ArgumentError,
            "unknown scenario #{inspect(opts.scenario)}. Available: #{Enum.join(Scenarios.names(), ", ")}"
    end

    opts
  end

  defp parse_sync(nil), do: nil
  defp parse_sync("none"), do: :none
  defp parse_sync("datasync"), do: :datasync
  defp parse_sync("fsync"), do: :fsync

  defp parse_sync(other) do
    raise ArgumentError, "unknown --sync #{inspect(other)}; use none, datasync, or fsync"
  end

  defp git_revision do
    case System.cmd("git", ["rev-parse", "--short", "HEAD"], stderr_to_stdout: true) do
      {output, 0} -> String.trim(output)
      _ -> "unknown"
    end
  rescue
    _ -> "unknown"
  end

  defp otp_release, do: :erlang.system_info(:otp_release) |> to_string()

  defp help do
    """
    Usage: mix run scripts/bench.exs [options]

      --scenario NAME      run one scenario (default: all)
      --iterations N       measured iterations per scenario (default: 5)
      --warmup N           warm-up iterations per scenario (default: 1)
      --scale F            scale scenario input sizes (default: 1.0)
      --sync METHOD        none | datasync | fsync (default: scenario-specific)
      --record             write results to the baseline file
      --baseline PATH      baseline path (default: #{@baseline_path})
      --compare PATH       compare results against a baseline and exit non-zero on regression
      --threshold PERCENT  regression threshold for --compare (default: 10.0)
      --json               print the report as JSON
      --help               show this help

    Scenarios: #{Enum.join(Scenarios.names(), ", ")}
    """
  end
end
