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

    test "removing the last component drops a node monitor" do
      node = :single_component@node
      monitors = Monitor.add(node, :machine, Monitor.init())
      monitors = Monitor.remove(node, :machine, monitors)

      assert monitors == %{}
      assert Monitor.components(node, monitors) == []
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

    test "remove keeps a pid monitor while other components remain" do
      pid = spawn(fn -> Process.sleep(1_000) end)
      monitors = Monitor.add(pid, :machine, Monitor.init())
      monitors = Monitor.add(pid, :aux, monitors)

      monitors = Monitor.remove(pid, :machine, monitors)

      assert Monitor.components(pid, monitors) == [:aux]
      assert map_size(monitors) == 1
    end

    test "node monitors keep and then drop components" do
      node = :multi_components@node
      monitors = Monitor.add(node, :machine, Monitor.init())
      monitors = Monitor.add(node, :aux, monitors)

      monitors = Monitor.remove(node, :machine, monitors)
      assert Monitor.components(node, monitors) == [:aux]

      {components, remaining} = Monitor.handle_down(node, monitors)
      assert components == [:aux]
      assert remaining == monitors
      assert Monitor.components(node, remaining) == [:aux]

      assert Monitor.remove(node, :aux, %{}) == %{}
    end

    test "handle_down reports all components of a pid" do
      pid = spawn(fn -> Process.sleep(1_000) end)
      monitors = Monitor.add(pid, :machine, Monitor.init())
      monitors = Monitor.add(pid, :aux, monitors)

      {components, remaining} = Monitor.handle_down(pid, monitors)

      assert Enum.sort(components) == [:aux, :machine]
      assert remaining == %{}
    end

    test "adding the current node emits a nodeup message" do
      node = node()
      _monitors = Monitor.add(node, :machine, Monitor.init())
      assert_receive {:nodeup, ^node, []}
    end
  end

  describe "Counters" do
    test "init is idempotent" do
      assert :ok = Counters.init()
      assert :ok = Counters.init()
    end

    test "new/fetch/delete/overview are ETS-backed or no-ops" do
      assert :ok = Counters.new(:counters_test, [{:count, 1, :counter, "Count"}])
      assert Counters.fetch(:counters_test) == nil
      assert Counters.overview() != nil
      assert Counters.overview(:counters_test) != nil
      assert Counters.counters(:counters_test, []) == %{}
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
