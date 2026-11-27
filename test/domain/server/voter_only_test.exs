defmodule RaftEx.Domain.Server.VoterOnlyTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.{Cluster, Config, Election}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log

  @self_id {:server1, node()}
  @peer2 {:server2, node()}
  @non_voter {:server4, node()}

  defp config do
    struct!(Config,
      id: @self_id,
      uid: "voter-only-uid",
      log_id: "voter-only-log",
      metrics_key: :voter_only_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{}}
    )
  end

  defp cluster_with_non_voter do
    %{
      @self_id => %{voter_status: %{membership: :voter}, match_index: 5},
      @peer2 => %{voter_status: %{membership: :voter}, match_index: 5},
      @non_voter => %{voter_status: %{membership: :non_voter}, match_index: 10}
    }
  end

  defp state(opts \\ []) do
    %{
      cfg: config(),
      current_term: 2,
      log: %Log{range: {0, 5}, last_term: 2},
      cluster: Keyword.get(opts, :cluster, cluster_with_non_voter()),
      membership: Keyword.get(opts, :membership, :voter)
    }
  end

  test "non-voter election timeout does not start an election" do
    non_voter_cluster = %{
      @self_id => %{voter_status: %{membership: :non_voter}, match_index: 5},
      @peer2 => %{voter_status: %{membership: :voter}, match_index: 5},
      @non_voter => %{voter_status: %{membership: :non_voter}, match_index: 10}
    }

    non_voter_state = state(cluster: non_voter_cluster, membership: :non_voter)

    assert {:follower, ^non_voter_state, []} =
             Server.handle_follower(:election_timeout, non_voter_state)
  end

  test "voter election timeout starts pre-vote" do
    voter_state = state(membership: :voter)

    assert {:pre_vote, _, _} = Server.handle_follower(:election_timeout, voter_state)
  end

  test "non-voter does not count its own vote" do
    non_voter_state = %{
      cfg: struct!(config(), id: @non_voter, uid: "non-voter-uid"),
      current_term: 2,
      log: %Log{range: {0, 5}, last_term: 2},
      cluster: cluster_with_non_voter(),
      membership: :non_voter
    }

    {ctx, _effects} = Election.start_election(:pre_vote, non_voter_state)

    refute MapSet.member?(ctx.votes_received, @non_voter)
    assert ctx.total_voters == 2
  end

  test "voter counts its own vote" do
    {ctx, _effects} = Election.start_election(:pre_vote, state())

    assert MapSet.member?(ctx.votes_received, @self_id)
    assert ctx.total_voters == 2
  end

  test "non-voter leader progress is not counted toward commit quorum" do
    cluster = cluster_with_non_voter()

    # Leader is @non_voter with last written 10; only voter matches (5,5) count.
    indexes = Cluster.voter_match_indexes(@non_voter, cluster, 10)

    refute 10 in indexes
    assert Enum.sort(indexes) == [5, 5]
    assert Cluster.agreed_commit(indexes) == 5
  end

  test "voter leader progress counts toward commit quorum" do
    cluster = cluster_with_non_voter()

    indexes = Cluster.voter_match_indexes(@self_id, cluster, 7)

    assert 7 in indexes
    assert Cluster.agreed_commit(indexes) == 5
  end
end
