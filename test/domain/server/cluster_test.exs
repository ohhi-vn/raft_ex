defmodule RaftEx.Domain.Server.ClusterTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Server.Cluster

  @id {:s1, node()}
  @peer2 {:s2, node()}
  @peer3 {:s3, node()}

  test "new_peer defaults to a voter" do
    peer = Cluster.new_peer()

    assert peer.voter_status == %{membership: :voter, uid: nil}
    assert peer.next_index == 1
    assert peer.match_index == 0
  end

  test "new/2 defaults every peer to a voter" do
    cluster = Cluster.new(@id, [@peer2, @peer3])

    assert Cluster.voters(cluster) == [@id, @peer2, @peer3] ||
             Enum.sort(Cluster.voters(cluster)) == Enum.sort([@id, @peer2, @peer3])

    assert Cluster.voter_count(cluster) == 3
  end

  test "voters and voter_count ignore non-voters and promotable members" do
    cluster = %{
      @id => Cluster.new_peer(),
      @peer2 =>
        Cluster.new_peer() |> Map.put(:voter_status, %{membership: :promotable, uid: nil}),
      @peer3 => Cluster.new_peer() |> Map.put(:voter_status, %{membership: :non_voter, uid: nil})
    }

    assert Cluster.voters(cluster) == [@id]
    assert Cluster.voter_count(cluster) == 1
  end

  test "merge_peer supplies voter_status for persisted peers that lack it" do
    cluster = Cluster.new(@id, %{@peer2 => %{next_index: 5}})
    peer = Cluster.get_peer(@peer2, cluster)

    assert peer.next_index == 5
    assert peer.voter_status == %{membership: :voter, uid: nil}
  end

  test "get_membership uses the stored role by server id" do
    cluster = %{
      @peer2 => %{Cluster.new_peer() | voter_status: %{membership: :non_voter, uid: "uid2"}}
    }

    assert Cluster.get_membership(cluster, @peer2, "uid2", :unknown) == :non_voter
    # Identity is the server id; a stale uid never hides the committed role.
    assert Cluster.get_membership(cluster, @peer2, "other", :unknown) == :non_voter
    assert Cluster.get_membership(%{}, @peer2, "uid2", :unknown) == :unknown
  end
end
