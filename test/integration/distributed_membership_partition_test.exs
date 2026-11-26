defmodule RaftEx.Distributed.MembershipPartitionTest do
  use ExUnit.Case, async: false

  @moduletag :distributed
  @moduletag timeout: 180_000

  alias RaftEx.Domain.Utils
  alias RaftEx.TestSupport.DistributedCluster, as: DC
  alias RaftEx.TestSupport.StateQuery

  setup do
    if Node.alive?() do
      peers = DC.start_peers(4)
      on_exit(fn -> DC.stop_peers(peers) end)
      %{peers: peers}
    else
      {:skip,
       "requires a named node: elixir --sname raft_test_node -S mix test --include distributed"}
    end
  end

  # Use a machine from test/support: it is compiled into the test ebin that is
  # on every peer's code path, unlike modules defined in this test file.
  defp machine, do: {:machine, RaftEx.TestSupport.LargeStateMachine, %{}}

  defp cluster(peers) do
    {name, leader, ids, _nodes} = DC.start_cluster(Enum.take(peers, 3), machine())

    # Raft only permits cluster changes once a no-op entry in the leader's own
    # term is committed, so commit one before any membership test begins.
    assert {:ok, _, _} = command(leader, 0)

    assert :ok =
             wait_until(
               fn -> if is_integer(state_of(leader)), do: :ok, else: :pending end,
               10_000
             )

    %{name: name, leader: leader, ids: ids, spare: Enum.at(peers, 3)}
  end

  defp command(server, delta) when is_integer(delta),
    do: write_command(server, {:increment, delta})

  defp command(server, cmd), do: DC.rpc(server, RaftEx, :process_command, [server, cmd, 5_000])

  # Distributed setup can make an occasional command exceed the RPC timeout;
  # retry it rather than failing an otherwise valid scenario.
  defp write_command(_server, _cmd, 0), do: {:error, :timeout}

  defp write_command(server, cmd, attempts \\ 5) do
    case DC.rpc(server, RaftEx, :process_command, [server, cmd, 5_000]) do
      {:ok, _, _} = reply ->
        reply

      _other ->
        Process.sleep(200)
        write_command(server, cmd, attempts - 1)
    end
  end

  # Read the *local* applied state on the node that owns the server. This is
  # what proves a partitioned follower actually caught up rather than just
  # forwarding the command to the leader.
  defp state_of(server) do
    case DC.rpc(server, RaftEx, :local_query, [server, StateQuery.fun(), 2_000]) do
      {:ok, %{counter: counter}, _leader} -> counter
      _ -> :unavailable
    end
  end

  # A leader that handles the command locally reports its registered name as an
  # atom; a follower redirecting the command reports the full {name, node} id.
  defp leader_name(server) do
    case command(server, {:get}) do
      {:ok, _, leader} when is_tuple(leader) -> elem(leader, 0)
      {:ok, _, leader} when is_atom(leader) -> leader
      _ -> nil
    end
  end

  defp find_leader(ids) do
    Enum.find_value(ids, fn id ->
      case DC.rpc(id, RaftEx, :ping, [id, 1_000]) do
        {:pong, :leader} -> id
        _ -> nil
      end
    end)
  end

  defp member_set(server) do
    case DC.rpc(server, RaftEx, :members, [server, 3_000]) do
      {:ok, members, _leader} -> MapSet.new(members)
      _ -> MapSet.new()
    end
  end

  defp wait_until(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    case fun.() do
      :ok ->
        :ok

      other ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          other
        else
          Process.sleep(min(50, remaining))
          do_wait_until(fun, deadline)
        end
    end
  end

  defp suspend(server) do
    :rpc.call(elem(server, 1), :sys, :suspend, [elem(server, 0)], 5_000)
  end

  defp resume(server) do
    :rpc.call(elem(server, 1), :sys, :resume, [elem(server, 0)], 5_000)
  end

  defp start_spare(%{name: name, ids: ids, spare: {_peer, node}}) do
    id = {:s4, node}
    uid = :rpc.call(node, RaftEx, :new_uid, [Utils.to_binary(name)])

    config = %{
      id: id,
      uid: uid,
      cluster_name: name,
      log_init_args: %{uid: uid},
      initial_members: ids,
      machine: machine()
    }

    :ok = :rpc.call(node, RaftEx, :start_server, [:default, config], 10_000)
    id
  end

  defp add_as_non_voter(leader, id) do
    assert {:ok, _, _} = DC.rpc(leader, RaftEx, :add_member, [leader, id, :non_voter, 10_000])

    # The joiner must be reachable on its own node. Membership propagation is
    # verified by the tests that write a command after joining.
    assert :ok =
             wait_until(
               fn -> if is_integer(state_of(id)), do: :ok, else: :pending end,
               20_000
             )
  end

  test "committed writes replicate to every node", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)

    for delta <- [1, 2, 3] do
      assert {:ok, _, ^leader} = command(leader, delta)
    end

    assert :ok =
             wait_until(
               fn -> if Enum.all?(ids, &(state_of(&1) == 6)), do: :ok, else: :pending end,
               10_000
             )
  end

  test "every node reports the same leader", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)

    for id <- ids do
      assert leader_name(id) == elem(leader, 0)
    end
  end

  test "the leader stays available while a follower is partitioned", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)
    follower = Enum.find(ids, &(&1 != leader))

    suspend(follower)
    assert {:pong, :leader} = DC.rpc(leader, RaftEx, :ping, [leader, 2_000])

    resume(follower)

    assert :ok =
             wait_until(
               fn -> if Enum.all?(ids, &is_integer(state_of(&1))), do: :ok, else: :pending end,
               10_000
             )
  end

  test "a partitioned leader is replaced and catches up when it returns", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)
    remaining = ids -- [leader]

    suspend(leader)
    assert :ok = DC.rpc(hd(remaining), RaftEx, :trigger_election, [hd(remaining), 5_000])

    new_leader =
      wait_until(
        fn ->
          case find_leader(remaining) do
            nil -> :pending
            found when found != leader -> found
            _found -> :pending
          end
        end,
        20_000
      )

    assert new_leader in remaining
    resume(leader)

    # Once the old leader is back the cluster must settle on a leader again and
    # every server must be readable (catching up applied state locally).
    assert :ok =
             wait_until(
               fn -> if find_leader(ids) != nil, do: :ok, else: :pending end,
               15_000
             )

    assert :ok =
             wait_until(
               fn -> if Enum.all?(ids, &is_integer(state_of(&1))), do: :ok, else: :pending end,
               10_000
             )
  end

  test "a single follower failure leaves the cluster writable", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)
    follower = hd(ids -- [leader])

    # Stopping a follower keeps a three-node cluster at its two-node quorum.
    :rpc.call(elem(follower, 1), :gen_statem, :stop, [elem(follower, 0)], 5_000)

    assert {:ok, _, ^leader} = command(leader, 7)
    assert is_integer(state_of(leader))
  end

  test "a new node can join the cluster as a non-voter", %{peers: peers} do
    ctx = cluster(peers)
    id = start_spare(ctx)

    add_as_non_voter(ctx.leader, id)

    assert {:pong, _role} = DC.rpc(id, RaftEx, :ping, [id, 2_000])
    assert is_integer(state_of(id))
  end

  test "joining an existing member is rejected", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)
    existing = hd(ids -- [leader])

    assert {:error, _reason} =
             DC.rpc(leader, RaftEx, :add_member, [leader, existing, :non_voter, 5_000])

    assert existing in member_set(leader)
  end

  test "removing a follower shrinks membership while writes continue", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)
    victim = hd(ids -- [leader])

    assert {:ok, _, ^leader} =
             DC.rpc(leader, RaftEx, :remove_member, [leader, victim, 10_000])

    assert :ok =
             wait_until(
               fn -> if victim in member_set(leader), do: :pending, else: :ok end,
               10_000
             )

    assert {:ok, _, ^leader} = command(leader, 3)
  end

  test "removing an unknown member is rejected", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)
    unknown = {:unknown, elem(hd(ids), 1)}

    assert {:error, _reason} = DC.rpc(leader, RaftEx, :remove_member, [leader, unknown, 5_000])
    assert MapSet.size(member_set(leader)) == length(ids)
  end

  test "a follower can take over leadership after an election", %{peers: peers} do
    %{leader: leader, ids: ids} = cluster(peers)
    remaining = ids -- [leader]

    assert :ok = DC.rpc(hd(remaining), RaftEx, :trigger_election, [hd(remaining), 5_000])

    new_leader =
      wait_until(
        fn ->
          case find_leader(remaining) do
            nil -> :pending
            found when found != leader -> found
            _found -> :pending
          end
        end,
        20_000
      )

    assert new_leader in remaining
    assert is_integer(state_of(new_leader))
  end
end
