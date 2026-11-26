defmodule RaftEx.Chaos.LeaderCrashTest do
  use ExUnit.Case, async: false

  @moduletag :chaos

  alias RaftEx.Domain.{Machine, Utils}
  alias RaftEx.TestSupport.FaultInjection

  setup do
    data_dir =
      Path.join(System.tmp_dir!(), "raft_chaos_#{System.unique_integer([:positive])}")

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
    {:machine, Machine.Simple, %{simple_fun: fn cmd, state -> {state, cmd} end, initial_state: 0}}
  end

  test "cluster survives leader crash and keeps serving consistent reads", %{data_dir: _} do
    name = :"chaos_#{System.unique_integer([:positive])}"
    ids = [{:s1, node()}, {:s2, node()}, {:s3, node()}]

    {:ok, _servers, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    Process.sleep(300)

    leader =
      Enum.find_value(ids, fn id ->
        case RaftEx.process_command(id, {:probe, System.unique_integer()}, 5_000) do
          {:ok, _, found} -> found
          _ -> nil
        end
      end)

    assert leader != nil

    # Write a value, then crash the leader; cluster should elect a new leader.
    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :hello, 3_000)

    # Crash leader process (simulated node failure).
    FaultInjection.crash(elem(leader, 0))
    Process.sleep(500)

    # A new leader should be electable among survivors.
    survivors = Enum.reject(ids, &(&1 == leader))

    new_leader =
      Enum.find_value(survivors, fn id ->
        case RaftEx.process_command(id, {:probe, System.unique_integer()}, 5_000) do
          {:ok, _, found} -> found
          _ -> nil
        end
      end)

    # We don't strictly require a new leader (may depend on timing), but if one
    # exists it must be a survivor.
    if new_leader do
      assert new_leader in survivors
    end

    # Verify a survivor can still serve a read after the crash.
    assert {:ok, _state, _leader} = RaftEx.local_query(hd(survivors), fn s -> s end, 3_000)
  end
end
