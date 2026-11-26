defmodule RaftExBenchTest do
  use ExUnit.Case, async: false

  @bench_dir Path.expand("../bench", __DIR__)

  Code.require_file(Path.join(@bench_dir, "json.ex"))
  Code.require_file(Path.join(@bench_dir, "scenarios.ex"))
  Code.require_file(Path.join(@bench_dir, "raft_ex_bench.ex"))

  @tiny %{iterations: 1, warmup: 0, scale: 0.001}

  describe "run/1" do
    test "every scenario produces one report row with the expected shape" do
      report = RaftExBench.run(@tiny)

      assert report["version"] == 1
      assert is_binary(report["generated_at"])
      assert is_binary(report["revision"])
      assert is_binary(report["otp"])
      assert is_binary(report["elixir"])
      assert length(report["scenarios"]) == length(RaftExBench.Scenarios.names())

      for scenario <- report["scenarios"] do
        assert scenario["name"] in RaftExBench.Scenarios.names()
        assert scenario["metric"] in ["throughput", "latency"]
        assert is_binary(scenario["unit"])
        assert scenario["iterations"] == 1
        assert scenario["warmup"] == 0
        assert scenario["input"] > 0
        assert scenario["elapsed_us"] >= 0
        assert is_float(scenario["per_iteration_us"])
        assert is_number(scenario["value"])
        assert is_binary(scenario["sync"])
      end
    end

    test "scenario selection runs only the requested scenario" do
      report = RaftExBench.run(Map.put(@tiny, :scenario, "log_fold"))
      assert [%{"name" => "log_fold"}] = report["scenarios"]
    end

    test "the report round-trips through JSON" do
      report = RaftExBench.run(Map.put(@tiny, :scenario, "query"))
      assert RaftExBench.JSON.decode(RaftExBench.JSON.encode(report)) == report
    end
  end

  describe "compare/3" do
    test "reports a throughput improvement with a zero exit code" do
      report = RaftExBench.run(Map.put(@tiny, :scenario, "wal_append_1uid"))
      [scenario] = report["scenarios"]
      assert scenario["value"] > 0
      baseline = put_value(report, scenario["value"] * 0.5)

      {rows, exit_code} = RaftExBench.compare(report, baseline, 10.0)

      assert exit_code == 0
      assert [%{"status" => "ok", "regression" => false, "change_pct" => pct}] = rows
      assert pct > 0
    end

    test "fails when throughput regresses beyond the threshold" do
      report = RaftExBench.run(Map.put(@tiny, :scenario, "wal_append_1uid"))
      [scenario] = report["scenarios"]
      baseline = put_value(report, scenario["value"] * 10)

      {rows, exit_code} = RaftExBench.compare(report, baseline, 10.0)

      assert exit_code == 1
      assert [%{"status" => "regression", "regression" => true}] = rows
    end

    test "fails when latency rises beyond the threshold" do
      report = RaftExBench.run(Map.put(@tiny, :scenario, "wal_recovery"))
      [scenario] = report["scenarios"]
      assert scenario["metric"] == "latency"
      assert scenario["value"] > 0
      baseline = put_value(report, scenario["value"] * 0.5)

      {rows, exit_code} = RaftExBench.compare(report, baseline, 10.0)

      assert exit_code == 1
      assert [%{"status" => "regression", "regression" => true, "change_pct" => pct}] = rows
      assert pct > 0
    end

    test "marks a scenario missing from the baseline as new" do
      report = RaftExBench.run(Map.put(@tiny, :scenario, "query"))
      baseline = %{"scenarios" => []}

      {rows, exit_code} = RaftExBench.compare(report, baseline, 10.0)

      assert exit_code == 0
      assert [%{"status" => "new", "regression" => false}] = rows
    end
  end

  defp put_value(report, value) do
    [scenario] = report["scenarios"]
    %{report | "scenarios" => [%{scenario | "value" => value}]}
  end
end
