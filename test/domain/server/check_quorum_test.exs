defmodule RaftEx.Domain.Server.CheckQuorumTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Infra.Persistence.Log

  @self_id {:server1, node()}
  @peer2 {:server2, node()}
  @peer3 {:server3, node()}

  defp config do
    struct!(Config,
      id: @self_id,
      uid: "quorum-uid",
      log_id: "quorum-log",
      metrics_key: :quorum_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{}}
    )
  end

  defp state(peer_acks, became_at) do
    cluster = %{
      @self_id => %{voter_status: %{membership: :voter}, match_index: 5},
      @peer2 => %{
        voter_status: %{membership: :voter},
        match_index: 5,
        last_ack: Map.get(peer_acks, @peer2, 0)
      },
      @peer3 => %{
        voter_status: %{membership: :voter},
        match_index: 5,
        last_ack: Map.get(peer_acks, @peer3, 0)
      }
    }

    %{
      cfg: config(),
      current_term: 3,
      leader_id: @self_id,
      cluster: cluster,
      log: %Log{entries: %{}, range: nil, last_term: 0, next_index: 1},
      became_leader_at: became_at
    }
  end

  test "lost majority steps down" do
    now = System.monotonic_time(:millisecond)
    # Became leader long ago, no peer has acked recently.
    st = state(%{}, now - 5_000)

    assert {:follower, new_state, []} = Server.check_quorum(st, now, 1_000)
    assert new_state.leader_id == nil
  end

  test "restored majority remains leader" do
    now = System.monotonic_time(:millisecond)
    st = state(%{@peer2 => now - 100}, now - 5_000)

    assert {:leader, _, []} = Server.check_quorum(st, now, 1_000)
  end

  test "grace period after election keeps leadership without acks" do
    now = System.monotonic_time(:millisecond)
    st = state(%{}, now - 100)

    assert {:leader, _, []} = Server.check_quorum(st, now, 1_000)
  end
end
