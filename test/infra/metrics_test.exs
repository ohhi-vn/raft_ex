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
