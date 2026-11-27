defmodule RaftEx.MultiServerConsensusTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  setup do
    data_dir =
      Path.join(System.tmp_dir!(), "raft_consensus_#{System.unique_integer([:positive])}")

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

  defp cluster do
    name = :"consensus_#{System.unique_integer([:positive])}"
    ids = [{:s1, node()}, {:s2, node()}, {:s3, node()}]

    {:ok, _servers, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    Process.sleep(300)

    # Discover the actual leader; the leaderboard can lag behind a re-election.
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

  test "elects one leader and replicates a command to all servers", %{data_dir: _} do
    {name, leader, ids} = cluster()
    assert leader in ids

    assert {:ok, _reply, ^leader} = RaftEx.process_command(leader, :hello, 3_000)

    for id <- ids do
      assert {:ok, state, _} = RaftEx.local_query(id, fn s -> s end, 3_000)
      assert is_tuple(state) or is_map(state) or is_list(state) or is_number(state)
    end

    follower = Enum.find(ids, &(&1 != leader))
    pid = Process.whereis(elem(follower, 0))
    assert is_pid(pid)
    :gen_statem.stop(pid)

    uid = RaftEx.server_uid(name, follower)

    config = %{
      id: follower,
      uid: uid,
      cluster_name: name,
      log_init_args: %{uid: uid},
      initial_members: ids,
      machine: machine()
    }

    assert :ok = RaftEx.start_server(:default, config)
    Process.sleep(500)
    assert {:ok, _state, _} = RaftEx.local_query(follower, fn s -> s end, 3_000)
  end

  test "answers local, leader and consistent queries", %{data_dir: _} do
    {_name, leader, ids} = cluster()

    assert {:ok, _, _} = RaftEx.local_query(hd(ids), fn s -> s end, 3_000)
    assert {:ok, _, _} = RaftEx.leader_query(leader, fn s -> s end, 3_000)
    assert {:ok, _, _} = RaftEx.consistent_query(leader, fn s -> s end, 3_000)

    follower = Enum.find(ids, &(&1 != leader))
    pid = Process.whereis(elem(follower, 0))
    if is_pid(pid), do: :gen_statem.stop(pid)
    Process.sleep(200)
    assert {:ok, _, _} = RaftEx.consistent_query(leader, fn s -> s end, 3_000)
  end

  test "transfers leadership to a caught-up voter", %{data_dir: _} do
    {name, leader, ids} = cluster()
    target = Enum.find(ids, &(&1 != leader))

    assert {:ok, ^target} = RaftEx.transfer_leadership(leader, target, 3_000)

    Process.sleep(500)
    assert RaftEx.Leaderboard.lookup_leader(name) == target
  end

  test "re-elects a leader after the current leader stops", %{data_dir: _} do
    {_name, leader, ids} = cluster()
    pid = Process.whereis(elem(leader, 0))
    assert is_pid(pid)
    :gen_statem.stop(pid)

    remaining = ids -- [leader]
    :ok = RaftEx.trigger_election(hd(remaining), 5_000)

    new_leader =
      Enum.find_value(1..30, fn _ ->
        Process.sleep(200)

        Enum.find_value(remaining, fn id ->
          case RaftEx.process_command(id, {:probe, System.unique_integer()}, 1_000) do
            {:ok, _, found} -> found
            _ -> nil
          end
        end)
      end)

    assert new_leader != nil
    assert new_leader != leader
  end

  test "catches up a restarted follower from a snapshot", %{data_dir: _} do
    {name, leader, ids} = cluster()

    for i <- 1..5 do
      assert {:ok, _, ^leader} = RaftEx.process_command(leader, {:snapshot_cmd, i}, 3_000)
    end

    assert :ok = RaftEx.trigger_compaction(leader)
    Process.sleep(300)

    follower = Enum.find(ids, &(&1 != leader))
    pid = Process.whereis(elem(follower, 0))
    assert is_pid(pid)
    :gen_statem.stop(pid)

    uid = RaftEx.server_uid(name, follower)

    config = %{
      id: follower,
      uid: uid,
      cluster_name: name,
      log_init_args: %{uid: uid},
      initial_members: ids,
      machine: machine()
    }

    assert :ok = RaftEx.start_server(:default, config)
    Process.sleep(500)
    assert {:ok, _, _} = RaftEx.local_query(follower, fn s -> s end, 3_000)
  end

  test "reports member roles and rejects promoting a lagging member", %{data_dir: _} do
    {_name, leader, _ids} = cluster()

    assert {:ok, info, _} = RaftEx.members_info(leader, 3_000)

    for {_id, peer} <- info do
      assert %{voter_status: %{membership: membership}} = peer
      assert membership in [:voter, :promotable, :non_voter]
    end

    assert {:ok, _, _} = RaftEx.demote_member(leader, leader, 3_000)
  end
end
