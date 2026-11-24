defmodule RaftEx.Infra.ObservabilityTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Observability.{Counters, MetricsEts, Monitor}

  describe "Monitor" do
    test "add and components for pids" do
      pid = spawn(fn -> Process.sleep(1_000) end)
      monitors = Monitor.add(pid, :machine, Monitor.init())

      assert Monitor.components(pid, monitors) == [:machine]

      monitors = Monitor.add(pid, :aux, monitors)
      assert Enum.sort(Monitor.components(pid, monitors)) == [:aux, :machine]
    end

    test "add and remove components for nodes" do
      monitors = Monitor.add(:some@node, :machine, Monitor.init())
      assert Monitor.components(:some@node, monitors) == [:machine]

      monitors = Monitor.add(:some@node, :aux, monitors)
      assert Enum.sort(Monitor.components(:some@node, monitors)) == [:aux, :machine]

      {components, remaining} = Monitor.handle_down(:some@node, monitors)
      assert Enum.sort(components) == [:aux, :machine]
      assert Enum.sort(Monitor.components(:some@node, remaining)) == [:aux, :machine]
    end

    test "remove drops the monitor when the last component is removed" do
      pid = spawn(fn -> Process.sleep(1_000) end)
      monitors = Monitor.add(pid, :machine, Monitor.init())
      monitors = Monitor.remove(pid, :machine, monitors)
      assert monitors == %{}
      assert Monitor.components(pid, monitors) == []
    end

    test "remove_all and unknown targets" do
      pid = spawn(fn -> Process.sleep(1_000) end)
      monitors = Monitor.add(pid, :machine, Monitor.init())
      monitors = Monitor.remove_all(:machine, monitors)
      assert monitors == %{}

      assert Monitor.remove(:unknown, :machine, %{}) == %{}
      assert Monitor.handle_down(:unknown, %{}) == {[], %{}}
      assert Monitor.components(:unknown, %{}) == []
    end
  end

  describe "Counters" do
    test "init is idempotent and counters can be managed" do
      assert :ok = Counters.init()
      assert :ok = Counters.init()

      assert :ok = Counters.new(:counters_test, [{:count, 1, :counter, "Count"}])
      assert Counters.fetch(:counters_test) != nil
      assert Counters.overview() != nil
      assert Counters.overview(:counters_test) != nil
      assert Counters.counters(:counters_test, []) != nil
      assert :ok = Counters.delete(:counters_test)
    end
  end

  describe "MetricsEts" do
    test "generic calls and casts are handled" do
      assert {:reply, :ok, %{}} = MetricsEts.handle_call(:anything, nil, %{})
      assert {:noreply, %{}} = MetricsEts.handle_cast(:anything, %{})
    end
  end
end
