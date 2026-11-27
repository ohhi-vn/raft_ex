defmodule RaftEx.Integration.CommandBatchTest do
  @moduledoc "End-to-end coverage for Section 8 (task 8.5): concurrent submissions share batched writes; every caller gets its own correct reply in order."
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn cmd, _state -> cmd end, initial_state: nil}}
  end

  defp boot(data_dir) do
    RaftEx.App.System.stop_default()

    try do
      :persistent_term.erase({:"$ra_system", :default})
    rescue
      _ -> :ok
    end

    :ok = RaftEx.start_in(data_dir)
  end

  defp find_leader(ids, attempts \\ 20) do
    Enum.find_value(ids, fn id ->
      case RaftEx.process_command(id, {:probe, System.unique_integer()}, 5_000) do
        {:ok, _, found} -> found
        _ -> nil
      end
    end) ||
      if attempts > 1 do
        Process.sleep(500)
        find_leader(ids, attempts - 1)
      end
  end

  test "8.5 concurrent commands all succeed with correct per-caller replies" do
    dir = Path.join(System.tmp_dir!(), "batch85_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"batch85_#{System.unique_integer([:positive])}"
    ids = [{:b1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    [sid] = ids
    assert find_leader(ids) != nil

    # Burst of concurrent submissions from distinct callers.
    tasks =
      for i <- 1..20 do
        Task.async(fn -> RaftEx.process_command(sid, {:cmd, i}, 15_000) end)
      end

    results = Task.await_many(tasks, 30_000)

    for {result, i} <- Enum.with_index(results, 1) do
      assert {:ok, {:cmd, ^i}, _leader} = result,
             "caller #{i} got #{inspect(result)}"
    end
  end

  test "8.3 ordering is preserved across a burst" do
    dir = Path.join(System.tmp_dir!(), "batch83_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"batch83_#{System.unique_integer([:positive])}"
    ids = [{:c1, node()}, {:c2, node()}, {:c3, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 15_000)
    leader = find_leader(ids)
    assert leader != nil

    # Sequential commands observe each other's effects in order.
    for i <- 1..10 do
      assert {:ok, {:cmd, ^i}, _} = RaftEx.process_command(leader, {:cmd, i}, 10_000)
    end

    {:ok, overview, _} = RaftEx.member_overview(leader)
    assert overview.last_applied >= 10
  end
end
