defmodule RaftEx.Domain.Server.LeaderStepdownTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log
  alias RaftEx.Infra.Persistence.LogMeta

  @self_id {:server1, node()}
  @peer2 {:server2, node()}
  @candidate {:candidate, node()}
  @leader2 {:leader2, node()}

  setup do
    dir = Path.join(System.tmp_dir!(), "stepdown_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    meta = :"stepdown_meta_#{System.unique_integer([:positive])}"

    {:ok, pid} = LogMeta.start_link(%{name: :stepdown, data_dir: dir, names: %{log_meta: meta}})
    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    {:ok, meta: meta}
  end

  defp config(meta) do
    struct!(Config,
      id: @self_id,
      uid: "stepdown-uid",
      log_id: "stepdown-log",
      metrics_key: :stepdown_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{log_meta: meta}}
    )
  end

  defp leader_state(meta) do
    %{
      cfg: config(meta),
      current_term: 2,
      leader_id: @self_id,
      voted_for: nil,
      rpc_from: @peer2,
      cluster: %{
        @self_id => %{
          voter_status: %{membership: :voter},
          next_index: 2,
          match_index: 1,
          query_index: 0,
          commit_index_sent: 0,
          status: :normal
        },
        @peer2 => %{
          voter_status: %{membership: :voter},
          next_index: 2,
          match_index: 1,
          query_index: 0,
          commit_index_sent: 0,
          status: :normal
        }
      },
      log: %Log{
        entries: %{1 => {1, 2, :cmd}},
        range: {1, 1},
        last_term: 2,
        next_index: 2,
        last_written_index_term: {1, 2}
      },
      pending_consistent_queries: [],
      pending_read_queries: [],
      queries_waiting_heartbeats: %{},
      commit_index: 1,
      last_applied: 1
    }
  end

  test "higher-term RequestVote steps the leader down", %{meta: meta} do
    rpc = %Types.RequestVoteRpc{
      term: 3,
      candidate_id: @candidate,
      last_log_index: 1,
      last_log_term: 2
    }

    assert {:follower, new_state, _} = Server.handle_leader(rpc, leader_state(meta))
    assert new_state.current_term == 3
  end

  test "higher-term AppendEntries steps the leader down", %{meta: meta} do
    rpc = %Types.AppendEntriesRpc{
      term: 3,
      leader_id: @leader2,
      prev_log_index: 0,
      prev_log_term: 0,
      entries: [],
      leader_commit: 0
    }

    assert {:follower, new_state, _} = Server.handle_leader(rpc, leader_state(meta))
    assert new_state.current_term == 3
  end

  test "higher-term AppendEntriesReply steps the leader down", %{meta: meta} do
    reply = %Types.AppendEntriesReply{
      term: 5,
      success: true,
      next_index: 2,
      last_index: 1,
      last_term: 2
    }

    assert {:follower, new_state, _} = Server.handle_leader(reply, leader_state(meta))
    assert new_state.current_term == 5
  end

  test "higher-term HeartbeatReply steps the leader down", %{meta: meta} do
    reply = %Types.HeartbeatReply{term: 4, query_index: 1}

    assert {:follower, new_state, _} = Server.handle_leader(reply, leader_state(meta))
    assert new_state.current_term == 4
  end

  test "same-term RequestVote does not step the leader down", %{meta: meta} do
    rpc = %Types.RequestVoteRpc{
      term: 2,
      candidate_id: @candidate,
      last_log_index: 1,
      last_log_term: 2
    }

    assert {:leader, new_state, _} = Server.handle_leader(rpc, leader_state(meta))
    assert new_state.current_term == 2
  end
end
