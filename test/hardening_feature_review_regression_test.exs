defmodule RaftEx.HardeningFeatureReviewRegressionTest do
  @moduledoc """
  Task 1.5: regression tests for feature-review findings.

  Each test asserts the CORRECT behavior. Tests for defects already fixed
  by Section 2 (non-voter campaigning, leader step-down) pass; tests for
  defects fixed later (shared uid, prior-term commit, higher-term granted
  vote) fail against the current implementation, documenting the gap.
  """
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server.{Cluster, Config, Election}
  alias RaftEx.Domain.Server.RpcHandler
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log

  @self_id {:server1, node()}
  @peer2 {:server2, node()}
  @peer3 {:server3, node()}
  @candidate {:candidate, node()}

  defp config(uid \\ "feature-review-uid") do
    struct!(Config,
      id: @self_id,
      uid: uid,
      log_id: "feature-review-log",
      metrics_key: :feature_review_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{log_meta: :feature_review_meta}}
    )
  end

  defp log_with(entries, last_written) do
    entries_map = Map.new(entries, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)

    %Log{
      entries: entries_map,
      range: {1, 2},
      last_term: 4,
      next_index: 3,
      last_written_index_term: last_written
    }
  end

  defp leader_state(cluster) do
    %{
      cfg: config(),
      current_term: 5,
      leader_id: @self_id,
      voted_for: nil,
      rpc_from: @peer2,
      cluster: cluster,
      log: log_with([{1, 5, :a}, {2, 4, :b}], {2, 4}),
      commit_index: 0,
      pending_consistent_queries: [],
      pending_read_queries: [],
      queries_waiting_heartbeats: %{},
      last_applied: 0
    }
  end

  defp voter_cluster do
    peer = fn match ->
      %{
        match_index: match,
        next_index: match + 1,
        query_index: 0,
        commit_index_sent: 0,
        status: :normal,
        voter_status: %{membership: :voter},
        in_flight: [],
        acked_in_flight: []
      }
    end

    %{@self_id => peer.(2), @peer2 => peer.(0), @peer3 => peer.(0)}
  end

  test "shared uid: servers in one cluster get distinct uids" do
    source = File.read!(Path.join([File.cwd!(), "lib", "raft_ex.ex"]))

    # Extract the start_cluster/5 clause that maps over server_ids and check
    # that the uid derivation inside it references the server id. A uid
    # derived from the cluster name alone is shared by every member.
    clause =
      source
      |> String.split("def start_cluster(system, cluster_name, machine, server_ids")
      |> Enum.at(1, "")
      |> String.split("\n")
      |> Enum.take_while(fn line -> not String.starts_with?(String.trim(line), "def ") end)
      |> Enum.join("\n")

    assert clause =~ "server_uid",
           "start_cluster/4 must derive a uid per server"

    assert clause =~ "id",
           "uid derivation must reference the server id, not just the cluster name"

    assert Regex.match?(~r/server_uid\([^)]*id/s, clause),
           "start_cluster/4 shares one uid across members; derive uid from cluster and server id"
  end

  test "prior-term entry is not committed by replica count alone" do
    # Leader term 5, log ends with a term-4 entry, majority ack index 2.
    reply = %Types.AppendEntriesReply{
      term: 5,
      success: true,
      next_index: 3,
      last_index: 2,
      last_term: 4
    }

    {new_state, _effects} =
      RpcHandler.handle_append_entries_reply(reply, leader_state(voter_cluster()))

    assert new_state.commit_index == 0,
           "prior-term entry must not commit by count alone; expected 0, got #{new_state.commit_index}"
  end

  test "non-voter does not campaign" do
    non_voter_state = %{
      cfg: struct!(config(), id: @peer3, uid: "non-voter-uid"),
      current_term: 2,
      membership: :non_voter,
      cluster: %{
        @self_id => %{voter_status: %{membership: :voter}},
        @peer2 => %{voter_status: %{membership: :voter}},
        @peer3 => %{voter_status: %{membership: :non_voter}}
      },
      log: %Log{range: {0, 5}, last_term: 2}
    }

    {ctx, _} = Election.start_election(:pre_vote, non_voter_state)
    refute MapSet.member?(ctx.votes_received, @peer3)
  end

  test "leader steps down on a higher term" do
    rpc = %Types.RequestVoteRpc{
      term: 9,
      candidate_id: @candidate,
      last_log_index: 1,
      last_log_term: 2
    }

    assert {:follower, new_state, _} =
             RaftEx.Domain.Server.handle_leader(rpc, leader_state(voter_cluster()))

    assert new_state.current_term == 9
  end

  test "granted higher-term vote reply steps the candidate down" do
    ctx = %{
      state: :candidate,
      term: 3,
      votes_received: MapSet.new([@self_id]),
      total_voters: 3,
      votes_rejected: MapSet.new(),
      last_log_index: 5,
      last_log_term: 3
    }

    reply = %Types.RequestVoteResult{term: 5, vote_granted: true}

    assert {:lost, _ctx, effects} = Election.handle_vote_reply(reply, ctx, %{}, @peer2)
    assert {:update_term, 5} in effects
    assert {:become, :follower} in effects
  end
end
