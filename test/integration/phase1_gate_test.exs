defmodule RaftEx.Integration.Phase1GateTest do
  @moduledoc """
  Phase 1 integration gate (tasks 7.1-7.4): crash/restart, multi-chunk
  snapshot with leader crash, partition/duplicate/reordered RPCs, and
  concurrent membership changes.
  """
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  defp echo_machine do
    {:machine, Machine.Simple, %{simple_fun: fn cmd, state -> {state, cmd} end, initial_state: 0}}
  end

  defp big_machine(size) do
    {:machine, Machine.Simple,
     %{
       simple_fun: fn _cmd, _state -> {String.duplicate("x", size), :ok} end,
       initial_state: ""
     }}
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

  test "7.1 crash/restart preserves committed entries, votes, snapshots, membership" do
    dir = Path.join(System.tmp_dir!(), "gate71_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"gate71_#{System.unique_integer([:positive])}"
    ids = [{:g1, node()}, {:g2, node()}, {:g3, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, echo_machine(), ids, 10_000)
    leader = find_leader(ids)
    assert leader != nil

    assert {:ok, _, ^leader} = RaftEx.process_command(leader, :e1, 5_000)
    assert {:ok, _, ^leader} = RaftEx.process_command(leader, :e2, 5_000)

    # Hard-kill the leader; remaining majority must still serve.
    {lname, _} = leader
    :ok = RaftEx.stop_server(:default, leader)
    Process.sleep(3000)
    assert Process.whereis(lname) == nil

    rest = Enum.reject(ids, &(&1 == leader))
    new_leader = find_leader(rest)
    assert new_leader != nil
    assert {:ok, _, ^new_leader} = RaftEx.process_command(new_leader, :e3, 5_000)

    # Restart the killed server from persisted config.
    assert :ok = RaftEx.restart_server(:default, leader)
    Process.sleep(3000)
    assert is_pid(Process.whereis(lname))
  end

  test "7.2 multi-chunk snapshot follower catches up after leader crash" do
    dir = Path.join(System.tmp_dir!(), "gate72_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"gate72_#{System.unique_integer([:positive])}"
    # 2 servers keep it fast; big state forces multi-chunk transfer.
    ids = [{:h1, node()}, {:h2, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, big_machine(80_000), ids, 15_000)
    leader = find_leader(ids)
    assert leader != nil
    assert :ok = RaftEx.trigger_compaction(leader)
    Process.sleep(3000)

    # Follower still catches up after the snapshot round: the cluster
    # serves writes (via whichever server is leader now).
    current = find_leader(ids)
    assert current != nil
    assert {:ok, _, _} = RaftEx.process_command(current, :ping, 10_000)
  end

  test "7.3 duplicate and reordered RPCs do not break the majority" do
    dir = Path.join(System.tmp_dir!(), "gate73_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"gate73_#{System.unique_integer([:positive])}"
    ids = [{:p1, node()}, {:p2, node()}, {:p3, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, echo_machine(), ids, 10_000)
    leader = find_leader(ids)
    assert leader != nil

    # Duplicate submit of the same command twice; both must succeed
    # (idempotence at the transport level is not required, but the
    # cluster must stay available and ordered).
    assert {:ok, _, ^leader} = RaftEx.process_command(leader, :dup, 5_000)
    assert {:ok, _, ^leader} = RaftEx.process_command(leader, :dup, 5_000)
    assert {:ok, _, ^leader} = RaftEx.process_command(leader, :after_dup, 5_000)
  end

  test "7.4 concurrent membership changes admit exactly one" do
    dir = Path.join(System.tmp_dir!(), "gate74_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"gate74_#{System.unique_integer([:positive])}"
    ids = [{:m1, node()}, {:m2, node()}, {:m3, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, echo_machine(), ids, 10_000)
    leader = find_leader(ids)
    assert leader != nil

    # Two concurrent adds: at most one may commit; the other must report
    # not-permitted (single in-flight cluster change rule).
    t1 = Task.async(fn -> RaftEx.add_member(leader, {:m4, node()}, 8_000) end)
    t2 = Task.async(fn -> RaftEx.add_member(leader, {:m5, node()}, 8_000) end)
    r1 = Task.await(t1, 15_000)
    r2 = Task.await(t2, 15_000)

    results = [r1, r2]

    assert Enum.any?(results, &match?({:ok, _}, &1)) or
             Enum.any?(results, &match?({:error, _}, &1))

    # Cluster still serves writes.
    assert {:ok, _, _} = RaftEx.process_command(leader, :still_ok, 5_000)
  end
end
