defmodule RaftEx.Infra.Observability.MetricsTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Observability.Metrics

  setup do
    Metrics.reset()
    :ok
  end

  test "counter increments" do
    Metrics.inc(:foo)
    Metrics.inc(:foo, 4)
    assert Metrics.get(:foo) == 5
  end

  test "concurrent increments are exact" do
    Metrics.reset()

    tasks =
      for _ <- 1..100 do
        Task.async(fn ->
          for _ <- 1..100, do: Metrics.inc(:concurrent_counter)
        end)
      end

    Task.await_many(tasks, :infinity)
    assert Metrics.get(:concurrent_counter) == 10_000
  end

  test "concurrent observations keep exact count and sum" do
    Metrics.reset()

    tasks =
      for _ <- 1..50 do
        Task.async(fn ->
          for i <- 1..20, do: Metrics.observe(:concurrent_hist, i)
        end)
      end

    Task.await_many(tasks, :infinity)
    h = Metrics.get(:concurrent_hist)
    assert h.count == 1_000
    assert h.sum == 50 * (20 * 21 / 2)
  end

  test "gauge sets" do
    Metrics.set(:bar, 3.5)
    assert Metrics.all()[{:gauge, :bar}] == 3.5
  end

  test "histogram accumulates" do
    Metrics.observe(:lat, 10)
    Metrics.observe(:lat, 20)
    h = Metrics.all()[{:hist, :lat}]
    assert h.count == 2
    assert h.sum == 30
    assert h.min == 10
    assert h.max == 20
  end
end
