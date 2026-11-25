defmodule RaftEx.LargeClusterTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  setup do
    data_dir =
      Path.join(System.tmp_dir!(), "raft_large_cluster_#{System.unique_integer([:positive])}")

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
    {:machine, Machine.Simple,
     %{
       simple_fun: fn
         cmd, state when is_integer(cmd) -> state + cmd
         _cmd, state -> state
       end,
       initial_state: 0
     }}
  end

  defp cluster(size) do
    prefix = :"large_#{System.unique_integer([:positive])}"
    name = :"#{prefix}_cluster"
    ids = for i <- 1..size, do: {:"#{prefix}_s#{i}", node()}

    on_exit(fn -> RaftEx.Leaderboard.clear(name) end)

    assert {:ok, started, []} = RaftEx.start_cluster(:default, name, machine(), ids, 2_000)
    assert length(started) == size

    leader = wait_until(fn -> discover_leader(ids) || :pending end, 10_000)
    assert leader in ids, "cluster #{inspect(name)} did not elect a leader"

    {name, leader, ids}
  end

  defp discover_leader(ids) do
    Enum.find_value(ids, fn id ->
      case RaftEx.process_command(id, {:probe, System.unique_integer()}, 1_000) do
        {:ok, _, leader} -> leader
        _ -> nil
      end
    end)
  end

  defp wait_until(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    case fun.() do
      :ok ->
        :ok

      result ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          result
        else
          Process.sleep(min(50, remaining))
          do_wait_until(fun, deadline)
        end
    end
  end

  defp applied_state(server_id) do
    case RaftEx.local_query(server_id, fn state -> state end, 1_000) do
      {:ok, state, _} when is_integer(state) -> state
      _ -> :unavailable
    end
  end

  defp stop_servers(ids) do
    Enum.each(ids, fn {name, _node} ->
      case Process.whereis(name) do
        nil -> :ok
        pid -> :gen_statem.stop(pid)
      end
    end)
  end

  defp wait_for_state(ids, expected) do
    wait_until(
      fn ->
        if Enum.all?(ids, &(applied_state(&1) == expected)), do: :ok, else: :pending
      end,
      5_000
    )
  end

  describe "five-node clusters" do
    test "elects one leader and replicates every committed command to all five servers" do
      {_name, leader, ids} = cluster(5)

      assert {:ok, members, _} = RaftEx.members(leader, 2_000)
      assert Enum.sort(members) == Enum.sort(ids)

      for delta <- [1, 2, 3] do
        assert {:ok, _, ^leader} = RaftEx.process_command(leader, delta, 3_000)
      end

      assert :ok = wait_for_state(ids, 6)
    end

    test "re-elects a leader after the leader stops and keeps accepting commands" do
      {_name, leader, ids} = cluster(5)

      assert {:ok, _, ^leader} = RaftEx.process_command(leader, 4, 3_000)
      stop_servers([leader])

      remaining = ids -- [leader]
      assert :ok = RaftEx.trigger_election(hd(remaining), 5_000)

      new_leader =
        wait_until(
          fn ->
            case discover_leader(remaining) do
              ^leader -> :pending
              nil -> :pending
              found -> found
            end
          end,
          10_000
        )

      assert new_leader in remaining
      assert {:ok, _, ^new_leader} = RaftEx.process_command(new_leader, 5, 3_000)
      assert :ok = wait_for_state(remaining, 9)
    end

    test "rejects duplicate joins and removal of unknown members" do
      {_name, leader, ids} = cluster(5)
      [first | _] = ids
      unknown = :"unknown_#{System.unique_integer([:positive])}"

      assert {:error, _reason} = RaftEx.add_member(leader, first, 2_000)
      assert {:error, _reason} = RaftEx.remove_member(leader, {unknown, node()}, 2_000)
      assert {:error, _reason} = RaftEx.promote_member(leader, {unknown, node()}, 2_000)
      assert {:ok, members, _} = RaftEx.members(leader, 2_000)
      assert Enum.sort(members) == Enum.sort(ids)
    end

    test "loses write quorum when three of five servers are stopped" do
      {_name, leader, ids} = cluster(5)
      followers = ids -- [leader]

      stop_servers(Enum.take(followers, 3))
      Process.sleep(500)

      result = RaftEx.process_command(leader, 99, 1_000)
      assert match?({:error, _}, result) or match?({:timeout, _}, result)
    end
  end

  describe "seven-node clusters" do
    test "elects one leader and replicates every committed command to all seven servers" do
      {_name, leader, ids} = cluster(7)

      assert {:ok, members, _} = RaftEx.members(leader, 3_000)
      assert Enum.sort(members) == Enum.sort(ids)

      for delta <- [1, 2, 3, 4] do
        assert {:ok, _, ^leader} = RaftEx.process_command(leader, delta, 3_000)
      end

      assert :ok = wait_for_state(ids, 10)
    end

    test "keeps committing with four of seven servers after three followers stop" do
      {_name, leader, ids} = cluster(7)
      followers = ids -- [leader]
      stopped = Enum.take(followers, 3)
      remaining = ids -- stopped

      stop_servers(stopped)
      Process.sleep(500)

      assert {:ok, _, ^leader} = RaftEx.process_command(leader, 7, 3_000)
      assert :ok = wait_for_state(remaining, 7)
    end

    test "stops committing when only three of seven servers remain" do
      {_name, leader, ids} = cluster(7)
      followers = ids -- [leader]

      stop_servers(Enum.take(followers, 4))
      Process.sleep(500)

      result = RaftEx.process_command(leader, 99, 1_000)
      assert match?({:error, _}, result) or match?({:timeout, _}, result)
    end
  end
end
