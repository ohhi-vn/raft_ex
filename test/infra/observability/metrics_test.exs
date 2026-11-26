defmodule RaftEx.Infra.Observability.MetricsCatalogueTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Observability.{MetricNames, Metrics}

  setup do
    Metrics.reset()
    :ok
  end

  test "catalogue is non-empty and unique" do
    names = MetricNames.names()
    assert names != []
    assert length(names) == length(Enum.uniq(names))
    assert Enum.all?(names, &is_atom/1)
  end

  test "every catalogue metric can be recorded and read" do
    for {name, kind} <- MetricNames.all() do
      case kind do
        :counter ->
          Metrics.inc(name)
          assert Metrics.get(name) == 1

        :gauge ->
          Metrics.set(name, 42)
          assert Metrics.get(name) == 42

        :histogram ->
          Metrics.observe(name, 10)
          h = Metrics.get(name)
          assert h.count == 1
      end
    end
  end

  test "counters are monotonic" do
    Metrics.inc(:elections_started)
    v1 = Metrics.get(:elections_started)
    Metrics.inc(:elections_started, 3)
    v2 = Metrics.get(:elections_started)
    assert v2 > v1
  end
end
