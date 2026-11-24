defmodule RaftEx.Domain.Server.ElectionTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server.{Config, Election}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log

  @uid "election-test-uid"
  @self_id {:server1, node()}
  @peer2 {:server2, node()}
  @peer3 {:server3, node()}
  @non_voter {:server4, node()}

  setup do
    # The table is owned by the test process and disappears when it exits.
    :ets.new(:election_test_meta, [:named_table, :public, :set])
    :ok
  end

  defp config do
    struct!(Config,
      id: @self_id,
      uid: @uid,
      log_id: "log",
      metrics_key: :election_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{log_meta: :election_test_meta}}
    )
  end

  defp cluster do
    %{
      @self_id => %{voter_status: %{membership: :voter}},
      @peer2 => %{voter_status: %{membership: :voter}},
      @peer3 => %{voter_status: %{membership: :promotable}},
      @non_voter => %{voter_status: %{membership: :non_voter}}
    }
  end

  defp state(opts \\ []) do
    %{
      cfg: config(),
      current_term: Keyword.get(opts, :term, 2),
      log: %Log{
        range: {0, Keyword.get(opts, :last_index, 5)},
        last_term: Keyword.get(opts, :last_term, 2)
      },
      cluster: Keyword.get(opts, :cluster, cluster())
    }
  end

  defp context(opts \\ []) do
    %{
      state: Keyword.get(opts, :state, :candidate),
      term: Keyword.get(opts, :term, 2),
      votes_received: Keyword.get(opts, :votes, MapSet.new([@self_id, @peer2])),
      total_voters: Keyword.get(opts, :total, 3),
      last_log_index: 5,
      last_log_term: 2
    }
  end

  describe "start_election/2" do
    test "pre-vote builds RPCs for voters without changing the term" do
      {ctx, effects} = Election.start_election(:pre_vote, state())

      assert ctx.state == :pre_vote
      assert ctx.term == 2
      assert ctx.votes_received == MapSet.new([@self_id])
      assert ctx.total_voters == 3
      assert ctx.last_log_index == 5
      assert ctx.last_log_term == 2
      assert length(effects) == 2

      for {:send_rpc, peer, rpc} <- effects do
        assert peer in [@peer2, @peer3]
        assert %Types.PreVoteRpc{} = rpc
        assert rpc.term == 2
        assert rpc.candidate_id == @self_id
        assert rpc.last_log_index == 5
        assert rpc.version == 1
        assert is_binary(rpc.token)
      end
    end

    test "candidate phase increments the term and stores the vote" do
      {ctx, effects} = Election.start_election(:candidate, state())

      assert ctx.state == :candidate
      assert ctx.term == 3

      assert Enum.take(effects, 2) == [
               {:store_meta, :current_term, 3},
               {:store_meta, :voted_for, @self_id}
             ]

      assert length(effects) == 4

      for {:send_rpc, _peer, %Types.RequestVoteRpc{} = rpc} <- Enum.drop(effects, 2) do
        assert rpc.term == 3
        assert rpc.candidate_id == @self_id
        assert rpc.last_log_term == 2
      end
    end

    test "an empty cluster produces no RPCs" do
      {_ctx, effects} = Election.start_election(:pre_vote, state(cluster: %{@self_id => %{}}))
      assert effects == []
    end
  end

  describe "handle_pre_vote_request/2" do
    test "grants when the term and log are up to date" do
      rpc = %Types.PreVoteRpc{term: 2, candidate_id: @peer2, last_log_index: 5, last_log_term: 2}
      assert {true, []} = Election.handle_pre_vote_request(rpc, state())
    end

    test "grants for a higher term and newer log" do
      rpc = %Types.PreVoteRpc{term: 3, candidate_id: @peer2, last_log_index: 8, last_log_term: 3}
      assert {true, []} = Election.handle_pre_vote_request(rpc, state())
    end

    test "denies for a stale term" do
      rpc = %Types.PreVoteRpc{term: 1, candidate_id: @peer2, last_log_index: 5, last_log_term: 2}
      assert {false, []} = Election.handle_pre_vote_request(rpc, state())
    end

    test "denies for a stale log" do
      rpc = %Types.PreVoteRpc{term: 2, candidate_id: @peer2, last_log_index: 4, last_log_term: 2}
      assert {false, []} = Election.handle_pre_vote_request(rpc, state())

      rpc = %Types.PreVoteRpc{term: 2, candidate_id: @peer2, last_log_index: 9, last_log_term: 1}
      assert {false, []} = Election.handle_pre_vote_request(rpc, state())
    end
  end

  describe "handle_vote_request/2" do
    test "denies a stale term without effects" do
      rpc = %Types.RequestVoteRpc{
        term: 1,
        candidate_id: @peer2,
        last_log_index: 5,
        last_log_term: 2
      }

      current = state()
      assert {false, ^current, []} = Election.handle_vote_request(rpc, current)
    end

    test "grants a higher term with an up-to-date log" do
      rpc = %Types.RequestVoteRpc{
        term: 3,
        candidate_id: @peer2,
        last_log_index: 6,
        last_log_term: 3
      }

      {granted, new_state, effects} = Election.handle_vote_request(rpc, state())

      assert granted
      assert new_state.current_term == 3

      assert effects == [
               {:store_meta, :current_term, 3},
               {:store_meta, :voted_for, @peer2}
             ]
    end

    test "denies a higher term with a stale log without updating the term" do
      rpc = %Types.RequestVoteRpc{
        term: 3,
        candidate_id: @peer2,
        last_log_index: 4,
        last_log_term: 2
      }

      {granted, new_state, effects} = Election.handle_vote_request(rpc, state())

      refute granted
      assert new_state.current_term == 2
      assert effects == []
    end

    test "same term grants when no vote was cast" do
      rpc = %Types.RequestVoteRpc{
        term: 2,
        candidate_id: @peer2,
        last_log_index: 5,
        last_log_term: 2
      }

      {granted, new_state, effects} = Election.handle_vote_request(rpc, state())

      assert granted
      assert new_state.current_term == 2
      assert length(effects) == 2
    end

    test "same term denies when already voted for another candidate" do
      :ets.insert(:election_test_meta, {@uid, 2, @peer3, 0})

      rpc = %Types.RequestVoteRpc{
        term: 2,
        candidate_id: @peer2,
        last_log_index: 5,
        last_log_term: 2
      }

      assert {false, _state, []} = Election.handle_vote_request(rpc, state())
    end

    test "same term grants when already voted for this candidate" do
      :ets.insert(:election_test_meta, {@uid, 2, @peer2, 0})

      rpc = %Types.RequestVoteRpc{
        term: 2,
        candidate_id: @peer2,
        last_log_index: 5,
        last_log_term: 2
      }

      {granted, _state, effects} = Election.handle_vote_request(rpc, state())

      assert granted
      assert length(effects) == 2
    end

    test "same term denies with a stale log" do
      rpc = %Types.RequestVoteRpc{
        term: 2,
        candidate_id: @peer2,
        last_log_index: 4,
        last_log_term: 2
      }

      assert {false, _state, []} = Election.handle_vote_request(rpc, state())
    end
  end

  describe "handle_vote_reply/4" do
    test "wins when the majority is reached" do
      result = %Types.RequestVoteResult{term: 2, vote_granted: true}

      assert {:won, ctx, [{:become, :leader}]} =
               Election.handle_vote_reply(result, context(), %{}, @peer3)

      assert MapSet.size(ctx.votes_received) == 3
    end

    test "continues waiting without a majority" do
      result = %Types.RequestVoteResult{term: 2, vote_granted: true}
      ctx = context(votes: MapSet.new([@self_id]), total: 5)
      assert {:ongoing, new_ctx, []} = Election.handle_vote_reply(result, ctx, %{}, @peer2)
      assert MapSet.size(new_ctx.votes_received) == 2
    end

    test "steps down when the peer has a higher term" do
      result = %Types.RequestVoteResult{term: 4, vote_granted: false}

      assert {:lost, _ctx, [{:become, :follower}, {:update_term, 4}]} =
               Election.handle_vote_reply(result, context(), %{}, @peer2)
    end

    test "keeps waiting when a vote is denied in the same term" do
      result = %Types.RequestVoteResult{term: 2, vote_granted: false}
      assert {:ongoing, _ctx, []} = Election.handle_vote_reply(result, context(), %{}, @peer2)
    end

    test "ignores a denied vote from a lower term" do
      result = %Types.RequestVoteResult{term: 1, vote_granted: false}
      assert {:ongoing, _ctx, []} = Election.handle_vote_reply(result, context(), %{}, @peer2)
    end
  end

  describe "handle_pre_vote_reply/4" do
    test "signals a real election after a successful pre-vote" do
      result = %Types.PreVoteResult{term: 2, vote_granted: true}
      ctx = context(state: :pre_vote)
      assert {true, new_ctx, []} = Election.handle_pre_vote_reply(result, ctx, %{}, @peer3)
      assert MapSet.size(new_ctx.votes_received) == 3
    end

    test "keeps probing without a majority" do
      result = %Types.PreVoteResult{term: 2, vote_granted: true}
      ctx = context(state: :pre_vote, votes: MapSet.new([@self_id]), total: 5)
      assert {false, _ctx, []} = Election.handle_pre_vote_reply(result, ctx, %{}, @peer2)
    end

    test "steps down when the peer has a higher term" do
      result = %Types.PreVoteResult{term: 4}
      ctx = context(state: :pre_vote)

      assert {false, _ctx, [{:become, :follower}, {:update_term, 4}]} =
               Election.handle_pre_vote_reply(result, ctx, %{}, @peer2)
    end

    test "handles a denied pre-vote" do
      result = %Types.PreVoteResult{term: 2, vote_granted: false}

      assert {false, _ctx, []} =
               Election.handle_pre_vote_reply(result, context(state: :pre_vote), %{}, @peer2)
    end
  end

  describe "quorum helpers" do
    test "evaluate_election_result/1" do
      assert Election.evaluate_election_result(context(votes: MapSet.new([@self_id, @peer2]))) ==
               :won

      assert Election.evaluate_election_result(context(votes: MapSet.new([@self_id]))) == :ongoing
    end

    test "can_still_win?/2 and votes_needed/1" do
      assert Election.can_still_win?(1, 3)
      refute Election.can_still_win?(2, 3)
      assert Election.votes_needed(3) == 2
      assert Election.votes_needed(5) == 3
    end
  end
end
