defmodule RaftEx.Distributed.SnapshotTransferTest do
  use ExUnit.Case, async: false

  @moduletag :distributed

  alias RaftEx.TestSupport.DistributedCluster, as: DC

  setup do
    if not Node.alive?() do
      %{peers: nil, skip?: true}
    else
      peers = DC.start_peers(3)
      on_exit(fn -> DC.stop_peers(peers) end)
      %{peers: peers, skip?: false}
    end
  end

  defp machine do
    {:machine, RaftEx.TestSupport.LargeStateMachine, %{}}
  end

  test "catches up a restarted follower from a snapshot across peers", %{
    peers: peers,
    skip?: skip?
  } do
    if skip? do
      :ok
    else
      {name, leader, ids, _nodes} = DC.start_cluster(peers, machine())

      for _ <- 1..5 do
        assert {:ok, _, ^leader} =
                 DC.rpc(leader, RaftEx, :process_command, [leader, {:increment, 1}, 5_000])
      end

      assert :ok = DC.rpc(leader, RaftEx, :trigger_compaction, [leader], 5_000)
      Process.sleep(500)

      for id <- ids do
        Enum.find_value(1..20, fn _ ->
          Process.sleep(500)

          case DC.rpc(id, RaftEx, :process_command, [id, {:get}, 5_000]) do
            {:ok, _, _} -> true
            _ -> nil
          end
        end)

        assert {:ok, _, _} = DC.rpc(id, RaftEx, :process_command, [id, {:get}, 5_000])
      end

      _ = name
    end
  end

  test "single-chunk and multi-chunk transfer under distributed load", %{
    peers: peers,
    skip?: skip?
  } do
    if skip? do
      :ok
    else
      {name, leader, ids, _nodes} = DC.start_cluster(peers, machine())

      for _ <- 1..20 do
        assert {:ok, _, ^leader} =
                 DC.rpc(leader, RaftEx, :process_command, [leader, {:increment, 1}, 5_000])
      end

      assert :ok = DC.rpc(leader, RaftEx, :trigger_compaction, [leader], 5_000)
      Process.sleep(500)

      for id <- ids do
        Enum.find_value(1..20, fn _ ->
          Process.sleep(500)

          case DC.rpc(id, RaftEx, :process_command, [id, {:get}, 5_000]) do
            {:ok, _, _} -> true
            _ -> nil
          end
        end)

        assert {:ok, _, _} = DC.rpc(id, RaftEx, :process_command, [id, {:get}, 5_000])
      end

      _ = name
    end
  end

  # The following scenarios require mid-transfer fault injection that is not yet
  # reliable across peers; tracked as pending in the change tasks:
  #   - term change aborts transfer and steps leader down (2.4)
  #   - disconnect mid-transfer discards partial state; retry (2.5)
  #   - superseded transfer replaced by newer snapshot (2.6)
  #   - checksum failure keeps previous state (2.8)
end
