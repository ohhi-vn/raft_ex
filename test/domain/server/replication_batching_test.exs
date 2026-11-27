defmodule RaftEx.Domain.ReplicationBatchingTest do
  @moduledoc "Unit coverage for Section 9 replication batching and acknowledgement tracking (tasks 9.1-9.9)."
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Domain.Server.RpcHandler
  alias RaftEx.Infra.Persistence.LogMeta

  @self_id {:server1, node()}
  @peer2 {:server2, node()}
  @peer3 {:server3, node()}

  setup do
    dir = RaftEx.TestSupport.tmp_dir("repl_batch_test")
    meta_name = :"repl_batch_meta_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogMeta, %{name: :repl_batch, names: %{log_meta: meta_name}, data_dir: dir}}
    )

    Process.put(:repl_batch_meta_name, meta_name)
    :ok
  end

  defp config(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "repl-batch-uid",
      log_id: "repl-batch-log",
      metrics_key: :repl_batch_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      command_plugs: [],
      rpc_plugs: [],
      machine_versions: [{0, 0}],
      max_pipeline_count: Keyword.get(opts, :max_pipeline_count, 4096),
      max_append_entries_rpc_batch_size:
        Keyword.get(opts, :max_append_entries_rpc_batch_size, 128),
      system_config: %{names: %{log_meta: Process.get(:repl_batch_meta_name)}}
    )
  end

  defp log(entries) do
    entries_map = Map.new(entries, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)
    last_index = entries |> Enum.map(&elem(&1, 0)) |> Enum.max(fn -> 0 end)

    %RaftEx.Infra.Persistence.Log{
      entries: entries_map,
      range: if(last_index == 0, do: nil, else: {1, last_index}),
      last_term: 2,
      next_index: last_index + 1,
      last_written_index_term: {last_index, 2},
      wal_name: nil,
      meta_name: Process.get(:repl_batch_meta_name),
      cfg: %{uid: "repl-batch-uid"}
    }
  end

  defp peer(next_index, opts \\ []) do
    %{
      voter_status: %{membership: :voter},
      next_index: next_index,
      match_index: Keyword.get(opts, :match_index, 0),
      query_index: 0,
      commit_index_sent: Keyword.get(opts, :commit_sent, 0),
      status: Keyword.get(opts, :status, :normal),
      in_flight: Keyword.get(opts, :in_flight, :queue.new()),
      acked_in_flight: Keyword.get(opts, :acked, MapSet.new())
    }
  end

  defp state(opts \\ []) do
    entries = Keyword.get(opts, :entries, [{1, 2, :a}, {2, 2, :b}, {3, 2, :c}])

    %{
      cfg: config(opts),
      current_term: 2,
      commit_index: Keyword.get(opts, :commit_index, 0),
      last_applied: 0,
      persisted_last_applied: 0,
      leader_id: nil,
      voted_for: nil,
      rpc_from: Keyword.get(opts, :rpc_from, nil),
      membership: :voter,
      cluster_change_permitted: true,
      cluster_index_term: {0, 0},
      commit_latency: 0,
      transfer_leadership: nil,
      became_leader_at: System.monotonic_time(:millisecond),
      pending_consistent_queries: [],
      query_index: 0,
      queries_waiting_heartbeats: :queue.new(),
      cluster:
        Keyword.get(opts, :cluster, %{
          @self_id => peer(4, match_index: 3),
          @peer2 => peer(Keyword.get(opts, :peer2_ni, 1)),
          @peer3 => peer(Keyword.get(opts, :peer3_ni, 1))
        }),
      log: log(entries),
      machine_state: {:simple, fn _cmd, s -> {s, :ok} end, 0},
      aux_state: nil
    }
  end

  defp send_rpcs(effects) do
    for {:send_rpc, peer, %Types.AppendEntriesRpc{} = rpc} <- effects, do: {peer, rpc}
  end

  test "9.1 batches are sized by max_append_entries_rpc_batch_size" do
    {_s, effects} = Server.make_rpcs(state(max_append_entries_rpc_batch_size: 2))
    rpcs = send_rpcs(effects)
    assert length(rpcs) == 2

    for {_peer, rpc} <- rpcs do
      assert length(rpc.entries) == 2
    end
  end

  test "9.5 one pass shares a single read across peers with different positions" do
    s = state(peer2_ni: 1, peer3_ni: 3, max_append_entries_rpc_batch_size: 128)
    {_s, effects} = Server.make_pipeline_rpcs(s)
    rpcs = Map.new(send_rpcs(effects))

    assert rpcs[@peer2].entries == [{1, 2, :a}, {2, 2, :b}, {3, 2, :c}]
    assert rpcs[@peer2].prev_log_index == 0
    assert rpcs[@peer3].entries == [{3, 2, :c}]
    assert rpcs[@peer3].prev_log_index == 2
    assert rpcs[@peer3].prev_log_term == 2
  end

  test "9.9 tick heartbeats current peers but sends nothing for snapshot/busy ones" do
    cluster = %{
      @self_id => peer(4, match_index: 3),
      @peer2 => peer(4, match_index: 3),
      @peer3 => peer(1, status: {:sending_snapshot, self(), 0})
    }

    {_s, effects} = Server.make_rpcs(state(cluster: cluster))
    rpcs = send_rpcs(effects)

    # Only the current peer gets a (heartbeat) RPC; the snapshot peer and
    # the leader itself get nothing.
    assert [{@peer2, rpc}] = rpcs
    assert rpc.entries == []
    assert rpc.prev_log_index == 3
  end

  test "9.2 a successful ack that leaves the peer behind schedules another pass" do
    reply = %Types.AppendEntriesReply{term: 2, success: true, next_index: 2, last_index: 1}

    {_state, effects} =
      RpcHandler.handle_append_entries_reply(reply, state(rpc_from: @peer2))

    assert Enum.any?(effects, &match?({:next_event, :info, :pipeline_rpcs}, &1))
  end

  test "9.2 a fully caught-up ack schedules no extra pass" do
    # peer2 at ni=4 already (log has 3 entries); ack the last index.
    reply = %Types.AppendEntriesReply{term: 2, success: true, next_index: 4, last_index: 3}

    {_state, effects} =
      RpcHandler.handle_append_entries_reply(
        reply,
        state(rpc_from: @peer2, peer2_ni: 4, commit_index: 3)
      )

    refute Enum.any?(effects, &match?({:next_event, _, :pipeline_rpcs}, &1))
  end

  test "9.3/9.4 acked sets stay bounded and in-flight stays a queue" do
    # A success reply confirms the follower through last_index, so match
    # jumps there directly and confirmed ranges leave the window.
    in_flight = :queue.from_list([{0, 1}, {1, 3}])

    cluster = %{
      @self_id => peer(4, match_index: 3),
      @peer2 => peer(1, match_index: 0, in_flight: in_flight)
    }

    reply = %Types.AppendEntriesReply{term: 2, success: true, next_index: 4, last_index: 3}

    {s1, _} =
      RpcHandler.handle_append_entries_reply(reply, state(rpc_from: @peer2, cluster: cluster))

    p1 = s1.cluster[@peer2]
    assert p1.match_index == 3
    assert :queue.is_queue(p1.in_flight)
    assert :queue.is_empty(p1.in_flight)
    assert MapSet.size(p1.acked_in_flight) == 0
  end

  test "9.6 commit advances without a broadcast effect" do
    # Two peers ack index 2 of a 2-entry log; commit must advance and apply
    # without emitting a broadcast heartbeat.
    base =
      state(
        entries: [{1, 2, :a}, {2, 2, :b}],
        cluster: %{
          @self_id => peer(3, match_index: 2),
          @peer2 => peer(3, match_index: 2),
          @peer3 => peer(3, match_index: 0)
        },
        rpc_from: @peer2
      )

    reply = %Types.AppendEntriesReply{term: 2, success: true, next_index: 3, last_index: 2}
    {s, effects} = RpcHandler.handle_append_entries_reply(reply, base)

    assert s.commit_index == 2
    assert Enum.any?(effects, &match?({:apply_committed, 2}, &1))
    refute Enum.any?(effects, &match?({:broadcast_commit, _}, &1))
  end

  test "9.7 prior-term entries never commit by replica count" do
    # Log: entries 1..2 in term 2, entry 3 in term 1... craft: current term 3,
    # all entries term 2 (prior). Even with full replication, no commit.
    base =
      state(
        entries: [{1, 2, :a}, {2, 2, :b}],
        cluster: %{
          @self_id => peer(3, match_index: 2),
          @peer2 => peer(3, match_index: 2),
          @peer3 => peer(3, match_index: 2)
        },
        rpc_from: @peer2
      )

    base = %{base | current_term: 3}
    reply = %Types.AppendEntriesReply{term: 3, success: true, next_index: 3, last_index: 2}
    {s, _effects} = RpcHandler.handle_append_entries_reply(reply, base)

    assert s.commit_index == 0
  end

  test "9.7 commit index never decreases when membership shrinks" do
    {advanced?, s} = Server.advance_commit_index(state(commit_index: 2), 1)
    assert advanced? == false
    assert s.commit_index == 2
  end
end
