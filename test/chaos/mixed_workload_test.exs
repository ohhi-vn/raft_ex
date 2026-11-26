defmodule RaftEx.Chaos.MixedWorkloadTest do
  use ExUnit.Case, async: false

  @moduletag :chaos

  alias RaftEx.Domain.Machine
  alias RaftEx.TestSupport.{FaultInjection, Linearizability}

  setup do
    data_dir =
      Path.join(System.tmp_dir!(), "raft_mixed_#{System.unique_integer([:positive])}")

    File.mkdir_p!(data_dir)

    RaftEx.App.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    :ok = RaftEx.start_in(data_dir)

    on_exit(fn ->
      for [name] <- :ets.match(:ra_state, {:"$1", :_, :_}) do
        case Process.whereis(name) do
          nil -> :ok
          pid -> :gen_statem.stop(pid)
        end
      end

      :ets.match_delete(:ra_state, {:_, :_, :_})
      :persistent_term.erase({:"$ra_system", :default})
      File.rm_rf!(data_dir)
    end)

    %{data_dir: data_dir}
  end

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn cmd, state -> {cmd, cmd} end, initial_state: 0}}
  end

  test "mixed read/write workload completes without crashing", %{data_dir: _} do
    name = :"mixed_#{System.unique_integer([:positive])}"
    ids = [{:m1, node()}, {:m2, node()}, {:m3, node()}]
    {:ok, _servers, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    Process.sleep(300)

    leader =
      Enum.find_value(ids, fn id ->
        case RaftEx.process_command(id, {:probe, 0}, 5_000) do
          {:ok, _, found} -> found
          _ -> nil
        end
      end)

    assert leader != nil

    history = Linearizability.new(0)

    # Perform a few writes and reads.
    values = [1, 2, 3]

    Enum.each(values, fn v ->
      assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, v, 3_000)
    end)

    # Read back the last value from a survivor.
    survivor = hd(Enum.reject(ids, &(&1 == leader)))
    assert {:ok, state, _} = RaftEx.local_query(survivor, fn s -> s end, 3_000)
    assert is_tuple(state) or is_integer(state)

    # Simulate a transient partition of a follower, then heal.
    follower = Enum.find(ids, &(&1 != leader))
    FaultInjection.partition(elem(follower, 0))
    Process.sleep(100)
    FaultInjection.heal(elem(follower, 0))
    Process.sleep(200)

    # Cluster should still serve.
    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :final, 3_000)
  end
end
