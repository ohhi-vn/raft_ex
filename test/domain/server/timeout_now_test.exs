defmodule RaftEx.Domain.Server.TimeoutNowTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log

  @self_id {:server1, node()}
  @leader {:leader, node()}
  @other {:other, node()}

  defp state(opts \\ []) do
    cfg =
      struct!(Config,
        id: @self_id,
        uid: "timeout-now-uid",
        log_id: "timeout-now-log",
        metrics_key: :timeout_now_metrics,
        machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
        machine_version: 0,
        effective_machine_version: 0,
        effective_machine_module: RaftEx.Domain.Machine.Simple,
        system_config: %{names: %{}}
      )

    %{
      cfg: cfg,
      current_term: 3,
      leader_id: Keyword.get(opts, :leader_id, @leader),
      voted_for: nil,
      cluster: %{},
      log: %Log{entries: %{}, range: nil, last_term: 0, next_index: 1}
    }
  end

  test "TimeoutNow from the recognized leader triggers an election" do
    rpc = %Types.TimeoutNowRpc{term: 3, leader_id: @leader}

    assert {:candidate, new_state, _effects} = Server.handle_follower(rpc, state())
    assert new_state.current_term == 3
  end

  test "TimeoutNow from a non-leader is rejected" do
    rpc = %Types.TimeoutNowRpc{term: 3, leader_id: @other}

    assert {:follower, new_state, []} = Server.handle_follower(rpc, state())
    assert new_state.current_term == 3
  end

  test "TimeoutNow with a different term is rejected" do
    rpc = %Types.TimeoutNowRpc{term: 4, leader_id: @leader}

    assert {:follower, new_state, []} = Server.handle_follower(rpc, state())
  end

  test "TimeoutNow with no recognized leader is rejected" do
    rpc = %Types.TimeoutNowRpc{term: 3, leader_id: @leader}

    assert {:follower, _, []} = Server.handle_follower(rpc, state(leader_id: nil))
  end
end
