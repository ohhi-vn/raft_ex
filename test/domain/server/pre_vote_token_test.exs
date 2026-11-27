defmodule RaftEx.Domain.Server.PreVoteTokenTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.{Config, Election}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.{Log, LogMeta}

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defp config(meta) do
    struct!(Config,
      id: @self_id,
      uid: "prevote-token-uid",
      log_id: "prevote-token-log",
      metrics_key: :prevote_token_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{log_meta: meta}}
    )
  end

  defp state(meta) do
    %{
      cfg: config(meta),
      current_term: 2,
      leader_id: nil,
      voted_for: nil,
      membership: :voter,
      cluster: %{
        @self_id => %{voter_status: %{membership: :voter}},
        @peer2 => %{voter_status: %{membership: :voter}}
      },
      log: %Log{entries: %{1 => {1, 2, :cmd}}, range: {1, 1}, last_term: 2, next_index: 2}
    }
  end

  test "failed pre-vote leaves the durable vote unchanged" do
    dir = Path.join(System.tmp_dir!(), "prevote_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    meta = :"prevote_meta_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      LogMeta.start_link(%{name: :prevote_test, data_dir: dir, names: %{log_meta: meta}})

    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    # No vote recorded yet.
    assert LogMeta.fetch(meta, "prevote-token-uid", :voted_for) == nil

    # Trigger a pre-vote (follower election timeout).
    assert {:pre_vote, _, _} = Server.handle_follower(:election_timeout, state(meta))

    # Pre-vote must not persist a self-vote.
    assert LogMeta.fetch(meta, "prevote-token-uid", :voted_for) == nil
  end

  test "stale pre-vote token reply is ignored" do
    ctx = %{
      state: :pre_vote,
      term: 2,
      token: "T2",
      votes_received: MapSet.new([@self_id]),
      votes_rejected: MapSet.new(),
      total_voters: 2,
      last_log_index: 1,
      last_log_term: 2
    }

    stale = %Types.PreVoteResult{term: 2, token: "T1", vote_granted: true}

    assert {false, ^ctx, []} = Election.handle_pre_vote_reply(stale, ctx, %{}, @peer2)
  end

  test "current pre-vote token reply is counted" do
    ctx = %{
      state: :pre_vote,
      term: 2,
      token: "T2",
      votes_received: MapSet.new([@self_id]),
      votes_rejected: MapSet.new(),
      total_voters: 2,
      last_log_index: 1,
      last_log_term: 2
    }

    current = %Types.PreVoteResult{term: 2, token: "T2", vote_granted: true}

    assert {true, new_ctx, []} = Election.handle_pre_vote_reply(current, ctx, %{}, @peer2)
    assert MapSet.member?(new_ctx.votes_received, @peer2)
  end
end
