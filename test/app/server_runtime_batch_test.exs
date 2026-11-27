defmodule RaftEx.App.ServerRuntimeBatchTest do
  @moduledoc "Unit coverage for Section 8 runtime batching: enqueue (8.1/8.2), backpressure (8.1), flush (8.5), step-down drain."
  use ExUnit.Case, async: false

  alias RaftEx.App.ServerRuntime
  alias RaftEx.Domain.Server.Config

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defp cfg(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "batch-rt-uid",
      log_id: "batch-rt-log",
      metrics_key: :batch_rt_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{},
      rpc_plugs: []
    )
  end

  defp server_state(opts \\ []) do
    %{
      cfg: cfg(opts),
      current_term: 2,
      commit_index: 0,
      last_applied: 0,
      persisted_last_applied: 0,
      leader_id: nil,
      voted_for: nil,
      membership: :voter,
      cluster_change_permitted: false,
      cluster_index_term: {0, 0},
      commit_latency: 0,
      transfer_leadership: nil,
      became_leader_at: System.monotonic_time(:millisecond),
      pending_consistent_queries: [],
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
      log: %RaftEx.Infra.Persistence.Log{
        entries: %{},
        range: nil,
        last_term: 0,
        next_index: 1,
        last_written_index_term: {0, 0},
        wal_name: nil,
        cfg: %{uid: "batch-rt-uid"}
      },
      machine_state: {:simple, fn _cmd, s -> {s, :ok} end, 0},
      aux_state: nil
    }
  end

  defp runtime_state(opts \\ []) do
    %{
      conf: %{
        name: :batch_rt_test,
        tick_timeout: 100,
        await_condition_timeout: 60_000,
        max_pending_commands: Keyword.get(opts, :max_pending_commands, 1024),
        command_batch_linger_ms: 60_000
      },
      server_state: server_state(opts),
      election_timeout_set: false,
      pending_commands: []
    }
  end

  defp usr_call(cmd, mode \\ :await_consensus) do
    {:leader_call, {:command, {:"$usr", cmd, mode}}}
  end

  test "8.1/8.2 user commands enqueue instead of appending immediately" do
    from1 = {self(), make_ref()}
    from2 = {self(), make_ref()}
    s = runtime_state()

    assert {:keep_state, s1, []} =
             ServerRuntime.leader({:call, from1}, usr_call(:a), s)

    assert {:keep_state, s2, []} =
             ServerRuntime.leader({:call, from2}, usr_call(:b), s1)

    q = Map.get(s2, :cmd_batch_queue)
    assert :queue.len(q) == 2
    assert Map.get(s2, :batch_flush_scheduled?) == true
    # Nothing appended yet; the batch goes out on flush.
    assert s2.server_state.log.entries == %{}

    # Linger is 60s in this test, so no flush fires on its own; the
    # scheduled flag proves one was armed.
  end

  test "8.1 overflow rejects the new command with backpressure" do
    from1 = {self(), make_ref()}
    from2 = {self(), make_ref()}
    s = runtime_state(max_pending_commands: 1)

    assert {:keep_state, s1, []} =
             ServerRuntime.leader({:call, from1}, usr_call(:a), s)

    assert {:keep_state, _s2, []} =
             ServerRuntime.leader({:call, from2}, usr_call(:b), s1)

    assert_receive {_, {:error, :backpressure}}, 1_000
  end

  test "flush drains the queue through one batch append" do
    from1 = {self(), make_ref()}
    s = runtime_state()

    assert {:keep_state, s1, []} =
             ServerRuntime.leader({:call, from1}, usr_call(:a), s)

    assert {:keep_state, s2, _actions} =
             ServerRuntime.leader(:info, :flush_commands, s1)

    assert :queue.is_empty(Map.get(s2, :cmd_batch_queue))
    assert Map.get(s2, :batch_flush_scheduled?) == false
    assert map_size(s2.server_state.log.entries) == 1
  end

  test "leaving leadership fails queued callers instead of hanging them" do
    from1 = {self(), make_ref()}
    s = runtime_state()

    assert {:keep_state, s1, []} =
             ServerRuntime.leader({:call, from1}, usr_call(:a), s)

    # Simulate losing quorum: a tick with a stale setup steps the leader
    # down to follower, which must fail the queued caller.
    stale =
      put_in(
        s1.server_state[:became_leader_at],
        System.monotonic_time(:millisecond) - 60_000
      )

    assert {:next_state, :follower, _s2, actions} =
             ServerRuntime.leader({:timeout, :tick}, :tick_timeout, %{
               stale
               | conf: Map.put(stale.conf, :election_timeout_max, 1)
             })

    assert Enum.any?(actions, fn
             {:reply, ^from1, {:error, :not_leader}} -> true
             _ -> false
           end)
  end
end
