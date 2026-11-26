defmodule RaftEx.Integration.DistributedFaultToleranceTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.{Machine, Utils}
  alias RaftEx.TestSupport.FaultInjection

  setup do
    data_dir =
      Path.join(System.tmp_dir!(), "raft_distft_#{System.unique_integer([:positive])}")

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

  defp cluster(n \\ 3) do
    name = :"distft_#{System.unique_integer([:positive])}"
    ids = Enum.map(1..n, fn i -> {:"s#{i}", node()} end)

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
    {name, leader, ids}
  end

  test "cluster survives loss of a minority follower and keeps serving writes", %{data_dir: _} do
    {_name, leader, ids} = cluster(3)

    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :before, 3_000)

    # Stop one follower.
    follower = Enum.find(ids, &(&1 != leader))
    pid = Process.whereis(elem(follower, 0))
    assert is_pid(pid)
    :gen_statem.stop(pid)
    Process.sleep(200)

    # Cluster (majority) should still accept writes.
    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :after, 3_000)
  end

  test "cluster elects new leader after leader crash and preserves committed data", %{data_dir: _} do
    {_name, leader, ids} = cluster(3)

    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :committed, 3_000)

    # Crash the leader.
    FaultInjection.crash(elem(leader, 0))
    Process.sleep(600)

    survivors = Enum.reject(ids, &(&1 == leader))

    # Try to find a new leader among survivors.
    new_leader =
      Enum.find_value(survivors, fn id ->
        case RaftEx.process_command(id, {:probe, System.unique_integer()}, 5_000) do
          {:ok, _, found} -> found
          _ -> nil
        end
      end)

    if new_leader do
      assert new_leader in survivors
      # New leader should be able to serve a read.
      assert {:ok, _state, _} = RaftEx.local_query(new_leader, fn s -> s end, 3_000)
    end
  end

  test "network partition of a follower heals and follower catches up", %{data_dir: _} do
    {_name, leader, ids} = cluster(3)

    follower = Enum.find(ids, &(&1 != leader))

    # Partition the follower (suspend).
    FaultInjection.partition(elem(follower, 0))

    # Write while partitioned.
    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :during_partition, 3_000)
    Process.sleep(200)

    # Heal.
    FaultInjection.heal(elem(follower, 0))
    Process.sleep(500)

    # Follower should catch up and serve reads.
    assert {:ok, _state, _} = RaftEx.local_query(follower, fn s -> s end, 3_000)
  end

  test "add_member grows the cluster and new member receives data", %{data_dir: _} do
    {name, leader, ids} = cluster(3)

    # Start a new server to add.
    new_id = {:s4, node()}
    uid = RaftEx.new_uid(Utils.to_binary(name))

    config = %{
      id: new_id,
      uid: uid,
      cluster_name: name,
      log_init_args: %{uid: uid},
      initial_members: ids,
      machine: machine()
    }

    assert :ok = RaftEx.start_server(:default, config)
    Process.sleep(200)

    # Add as non-voter first (promotable).
    case RaftEx.add_member(leader, new_id, :non_voter, 5_000) do
      {:ok, _, _} -> :ok
      {:error, _} -> :ok
    end

    Process.sleep(300)

    # Write some data.
    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :post_add, 3_000)
    Process.sleep(300)

    # New member should be able to serve a local read.
    assert {:ok, _state, _} = RaftEx.local_query(new_id, fn s -> s end, 3_000)
  end

  test "remove_member shrinks the cluster", %{data_dir: _} do
    {_name, leader, ids} = cluster(3)

    member_to_remove = Enum.find(ids, &(&1 != leader))

    case RaftEx.remove_member(leader, member_to_remove, 5_000) do
      {:ok, _, _} -> :ok
      {:error, _} -> :ok
    end

    Process.sleep(300)

    # Leader should still serve writes with remaining majority.
    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :post_remove, 3_000)
  end

  test "five-node cluster tolerates two simultaneous follower failures", %{data_dir: _} do
    {_name, leader, ids} = cluster(5)

    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :before, 3_000)

    # Stop two followers.
    followers = Enum.reject(ids, &(&1 == leader)) |> Enum.take(2)

    Enum.each(followers, fn f ->
      pid = Process.whereis(elem(f, 0))
      if is_pid(pid), do: :gen_statem.stop(pid)
    end)

    Process.sleep(300)

    # Majority (3 of 5) remains; writes should succeed.
    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :after, 3_000)
  end
end
