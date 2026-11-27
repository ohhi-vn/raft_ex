defmodule RaftEx.App.ServerRuntimeElectionEffectsTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.ServerRuntime
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.{Log, LogMeta}

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  setup do
    dir = Path.join(System.tmp_dir!(), "runtime_election_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    meta = :"election_eff_meta_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      LogMeta.start_link(%{name: :election_eff, data_dir: dir, names: %{log_meta: meta}})

    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    {:ok, meta: meta}
  end

  defp server_state(meta, term) do
    cfg =
      struct!(Config,
        id: @self_id,
        uid: "election-eff-uid",
        log_id: "election-eff-log",
        metrics_key: :election_eff_metrics,
        machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
        machine_version: 0,
        effective_machine_version: 0,
        effective_machine_module: RaftEx.Domain.Machine.Simple,
        system_config: %{data_dir: "/tmp/election_eff", names: %{log_meta: meta}}
      )

    %{
      cfg: cfg,
      current_term: term,
      commit_index: 0,
      last_applied: 0,
      persisted_last_applied: 0,
      leader_id: nil,
      voted_for: nil,
      membership: :voter,
      cluster_change_permitted: true,
      pending_consistent_queries: [],
      pending_read_queries: [],
      queries_waiting_heartbeats: %{},
      rpc_from: @peer2,
      cluster: %{
        @self_id => %{
          voter_status: %{membership: :voter},
          next_index: 1,
          match_index: 0,
          query_index: 0,
          commit_index_sent: 0,
          status: :normal
        },
        @peer2 => %{
          voter_status: %{membership: :voter},
          next_index: 1,
          match_index: 0,
          query_index: 0,
          commit_index_sent: 0,
          status: :normal
        }
      },
      log: %Log{entries: %{}, range: nil, last_term: 0, next_index: 1},
      machine_state: nil,
      aux_state: nil
    }
  end

  defp runtime_state(meta, election_state, term) do
    %{
      conf: %{
        name: :election_eff_test,
        cluster_name: :election_eff_cluster,
        data_dir: "/tmp/election_eff",
        log_id: "election-eff-log",
        broadcast_time: 100,
        tick_timeout: 100
      },
      server_state: server_state(meta, term),
      election_timeout_set: false,
      pending_commands: [],
      election_context: %{
        state: election_state,
        term: term,
        votes_received: MapSet.new([@self_id]),
        total_voters: 2,
        votes_rejected: MapSet.new(),
        last_log_index: 0,
        last_log_term: 0
      }
    }
  end

  test "candidate adopts higher term from a vote reply and becomes follower", %{meta: meta} do
    base = runtime_state(meta, :candidate, 2)
    from = {self(), make_ref()}
    reply = %Types.RequestVoteResult{term: 5, vote_granted: false}

    assert {:next_state, :follower, new_state, _actions} =
             ServerRuntime.candidate({:cast, {from, reply}}, :vote_reply, base)

    assert new_state.server_state.current_term == 5
    assert new_state.server_state.voted_for == nil
    assert LogMeta.fetch(meta, "election-eff-uid", :current_term) == 5
  end

  test "pre-vote candidate adopts higher term and leaves pre-vote", %{meta: meta} do
    base = runtime_state(meta, :pre_vote, 2)
    from = {self(), make_ref()}
    reply = %Types.PreVoteResult{term: 5, vote_granted: true, token: "t"}

    assert {:next_state, :follower, new_state, _actions} =
             ServerRuntime.pre_vote({:cast, {from, reply}}, :pre_vote_reply, base)

    assert new_state.server_state.current_term == 5
    assert LogMeta.fetch(meta, "election-eff-uid", :current_term) == 5
  end
end
