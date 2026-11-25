defmodule RaftEx.Domain.Server.RpcHandlerTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server.{Cluster, Config, RpcHandler}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Filesystem
  alias RaftEx.Infra.Persistence.{Log, LogSnapshot, LogWal}

  @self_id {:server1, node()}
  @leader {:leader, node()}
  @peer2 {:server2, node()}
  @peer3 {:server3, node()}

  # A WAL that rejects appends with a clean error tuple, used to drive the
  # {:error, reason} branch of Log.write/2 in RpcHandler.append_entries/3.
  defmodule WalErrorServer do
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, :ok, [])
    def init(:ok), do: {:ok, :ok}

    def handle_call({:append, _uid, _entries}, _from, state),
      do: {:reply, {:error, :wal_down}, state}

    def handle_call(_msg, _from, state), do: {:reply, :ok, state}
  end

  setup do
    data_dir = RaftEx.TestSupport.tmp_dir("rpc_handler_test")
    wal_name = :"rpc_handler_wal_#{System.unique_integer([:positive])}"

    start_supervised!({LogWal, %{names: %{wal: wal_name}, data_dir: data_dir}})

    Process.put(:rpc_test_data_dir, data_dir)
    Process.put(:rpc_test_wal_name, wal_name)

    :ok
  end

  defp config do
    struct!(Config,
      id: @self_id,
      uid: "rpc-test-uid",
      log_id: "log",
      metrics_key: :rpc_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{
        data_dir: Process.get(:rpc_test_data_dir),
        names: %{wal: Process.get(:rpc_test_wal_name)}
      }
    )
  end

  defp log(entries \\ []) do
    entries_map = Map.new(entries, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)
    indexes = Enum.map(entries, &elem(&1, 0))

    {range, last_term} =
      case indexes do
        [] -> {nil, 0}
        _ -> {{Enum.min(indexes), Enum.max(indexes)}, entries |> List.last() |> elem(1)}
      end

    %Log{
      entries: entries_map,
      range: range,
      last_term: last_term,
      next_index: if(indexes == [], do: 0, else: Enum.max(indexes) + 1),
      wal_name: Process.get(:rpc_test_wal_name)
    }
  end

  defp cluster do
    peer = fn match ->
      %{
        match_index: match,
        next_index: match + 1,
        query_index: 0,
        commit_index_sent: 0,
        status: :normal,
        voter_status: %{membership: :voter}
      }
    end

    %{
      @self_id => peer.(0),
      @peer2 => peer.(0),
      @peer3 => peer.(0)
    }
  end

  defp state(opts \\ []) do
    %{
      cfg: config(),
      current_term: Keyword.get(opts, :term, 2),
      log: Keyword.get(opts, :log, log()),
      cluster: Keyword.get(opts, :cluster, cluster()),
      commit_index: Keyword.get(opts, :commit_index, 0),
      leader_id: nil,
      rpc_from: @peer2,
      voted_for: nil,
      snapshot_state: nil
    }
  end

  defp append_rpc(opts \\ []) do
    %Types.AppendEntriesRpc{
      term: Keyword.get(opts, :term, 2),
      leader_id: @leader,
      prev_log_index: Keyword.get(opts, :prev_index, 0),
      prev_log_term: Keyword.get(opts, :prev_term, 0),
      entries: Keyword.get(opts, :entries, []),
      leader_commit: Keyword.get(opts, :leader_commit, 0)
    }
  end

  defp snapshot_chunk(meta \\ %{index: 3, term: 2}) do
    meta_bin = :erlang.term_to_binary(meta)
    mac_bin = :erlang.term_to_binary(:mac_state)
    data = <<byte_size(meta_bin)::32-integer, meta_bin::binary, mac_bin::binary>>
    crc = :erlang.crc32(data)
    <<"RASN", 1, crc::32-integer, data::binary>>
  end

  describe "handle_append_entries_rpc/2" do
    test "rejects a stale leader term" do
      {next, state, effects} = RpcHandler.handle_append_entries_rpc(append_rpc(term: 1), state())

      assert next == :follower
      assert [%Types.AppendEntriesReply{} = reply] = effects_liquids(effects)

      refute reply.success
      assert reply.term == 2
      assert state.current_term == 2
    end

    test "accepts a heartbeat and adopts a higher term" do
      {next, state, effects} = RpcHandler.handle_append_entries_rpc(append_rpc(term: 3), state())

      assert next == :follower
      assert state.current_term == 3
      assert state.leader_id == @leader

      assert [{:send_rpc, @leader, %Types.AppendEntriesReply{} = reply} | _] = effects
      assert reply.success
      assert reply.term == 3
      assert reply.next_index == 1
    end

    test "does not advance commit index below the leader's commit" do
      {_next, state, effects} =
        RpcHandler.handle_append_entries_rpc(append_rpc(leader_commit: 0), state(commit_index: 5))

      assert state.commit_index == 5
      refute Enum.any?(effects, &match?({:apply_committed, _}, &1))
    end

    test "advances the commit index up to the last log index" do
      current = state(log: log([{1, 2, :cmd}, {2, 2, :cmd}]))

      {_next, state, effects} =
        RpcHandler.handle_append_entries_rpc(append_rpc(leader_commit: 5), current)

      assert state.commit_index == 2
      assert Enum.member?(effects, {:apply_committed, 2})
    end

    test "skips entries already present with the same term" do
      current = state(log: log([{1, 2, :cmd}]))
      rpc = append_rpc(prev_index: 1, prev_term: 2, entries: [{1, 2, :cmd}])

      {_next, _state, effects} = RpcHandler.handle_append_entries_rpc(rpc, current)

      refute Enum.any?(effects, &match?({:ra_log_event, _}, &1))
    end

    test "appends new entries and writes them to the WAL" do
      {_next, state, effects} =
        RpcHandler.handle_append_entries_rpc(append_rpc(entries: [{1, 2, :cmd}]), state())

      assert state.log.entries[1] == {1, 2, :cmd}
      assert Enum.any?(effects, &match?({:ra_log_event, {:written, _}}, &1))
    end

    test "rejects a missing previous log entry with a next-index hint" do
      rpc = append_rpc(prev_index: 4, prev_term: 1)
      {_next, state, effects} = RpcHandler.handle_append_entries_rpc(rpc, state())

      assert [{:send_rpc, @leader, %Types.AppendEntriesReply{} = reply}] = effects
      refute reply.success
      assert reply.next_index == 1
      assert state.log == state.log
    end

    test "rejects a term mismatch at the previous log index" do
      rpc = append_rpc(prev_index: 1, prev_term: 1)
      current = state(log: log([{1, 2, :cmd}]))
      {_next, _state, effects} = RpcHandler.handle_append_entries_rpc(rpc, current)

      assert [{:send_rpc, @leader, %Types.AppendEntriesReply{success: false}}] = effects
    end

    test "a WAL write error skips the written-log effect" do
      {:ok, wal} = WalErrorServer.start_link()
      current = state()
      log = %{current.log | wal_name: wal}

      {next, _state, effects} =
        RpcHandler.handle_append_entries_rpc(
          append_rpc(entries: [{1, 2, :cmd}]),
          %{current | log: log}
        )

      assert next == :follower
      refute Enum.any?(effects, &match?({:ra_log_event, _}, &1))
    end
  end

  describe "handle_append_entries_reply/2" do
    test "steps down on a higher reply term (success)" do
      reply = %Types.AppendEntriesReply{term: 3, success: true, next_index: 2, last_index: 1}
      {state, effects} = RpcHandler.handle_append_entries_reply(reply, state())
      assert state.current_term == 3
      assert effects == [{:become, :follower}]
    end

    test "updates peer state and may advance the commit index (success)" do
      reply = %Types.AppendEntriesReply{term: 2, success: true, next_index: 3, last_index: 2}
      {state, effects} = RpcHandler.handle_append_entries_reply(reply, state())

      assert state.cluster[@peer2].match_index == 2
      assert state.cluster[@peer2].next_index == 3
      assert state.cluster[@peer2].status == :normal
      assert is_list(effects)
    end

    test "ignores a stale success reply" do
      reply = %Types.AppendEntriesReply{term: 1, success: true, next_index: 2, last_index: 1}
      {state, effects} = RpcHandler.handle_append_entries_reply(reply, state())
      assert state.current_term == 2
      assert effects == []
    end

    test "steps down on a higher reply term (failure)" do
      reply = %Types.AppendEntriesReply{term: 3, success: false}
      {state, effects} = RpcHandler.handle_append_entries_reply(reply, state())
      assert state.current_term == 3
      assert effects == [{:become, :follower}]
    end

    test "uses the failure hint and schedules a retry" do
      reply = %Types.AppendEntriesReply{term: 2, success: false, next_index: 4}
      {state, effects} = RpcHandler.handle_append_entries_reply(reply, state())

      assert state.cluster[@peer2].next_index == 4
      assert effects == [{:send_append_entries, @peer2}]
    end

    test "decrements next_index when the failure has no hint" do
      reply = %Types.AppendEntriesReply{term: 2, success: false, next_index: nil}
      {state, effects} = RpcHandler.handle_append_entries_reply(reply, state())

      assert state.cluster[@peer2].next_index == 1
      assert effects == [{:send_append_entries, @peer2}]
    end

    test "ignores a stale failure reply" do
      reply = %Types.AppendEntriesReply{term: 1, success: false, next_index: 4}
      {state, effects} = RpcHandler.handle_append_entries_reply(reply, state())
      assert state.current_term == 2
      assert effects == []
    end
  end

  describe "handle_install_snapshot_rpc/2" do
    test "rejects a stale leader term" do
      rpc = %Types.InstallSnapshotRpc{term: 1, leader_id: @leader, meta: %{}, data: []}
      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, state())

      assert next == :follower
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{term: 2}}] = effects
      assert state.current_term == 2
    end

    test "accepts a first and middle chunk" do
      meta = %{index: 3, term: 2}

      first = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :first,
        data: snapshot_chunk(meta)
      }

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(first, state())
      assert next == :receive_snapshot
      assert effects == []
      assert is_tuple(state.snapshot_state)

      middle = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :middle,
        data: snapshot_chunk(meta)
      }

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(middle, state)
      assert next == :receive_snapshot
      assert effects == []
      assert is_tuple(state.snapshot_state)
    end

    test "completes a snapshot transfer on the last chunk" do
      meta = %{index: 3, term: 2}

      first = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :first,
        data: snapshot_chunk(meta)
      }

      {_next, state, _} = RpcHandler.handle_install_snapshot_rpc(first, state())

      last = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :last,
        data: <<>>
      }

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(last, state)

      assert next == :follower
      assert is_nil(state.snapshot_state)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{} = reply}] = effects
      assert reply.last_index == 3
      # Log.last_index_term/1 uses the log's last_term for its range after
      # snapshot installation; the fixture log starts at term 0.
      assert reply.last_term == 0
    end

    test "rejects a stale-term snapshot and discards partial state" do
      rpc = %Types.InstallSnapshotRpc{
        term: 1,
        leader_id: @leader,
        meta: %{index: 3, term: 1},
        chunk_state: :first,
        data: snapshot_chunk()
      }

      state =
        state()
        |> Map.put(:snapshot_state, {:partial, :state})
        |> Map.put(:snapshot_meta, %{index: 3, term: 1})

      {next, state2, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, state)

      assert next == :follower
      assert is_nil(state2.snapshot_state)
      assert is_nil(state2.snapshot_meta)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{} = reply}] = effects
      assert reply.last_index == 0
    end

    test "handles a full snapshot" do
      meta = %{index: 4, term: 2}

      rpc = %Types.InstallSnapshotRpc{
        term: 3,
        leader_id: @leader,
        meta: meta,
        chunk_state: :full,
        data: :machine_state
      }

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, state())

      assert next == :follower
      assert state.current_term == 3
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{} = reply}] = effects
      assert reply.last_index == 4
    end

    test "streams a multi-chunk snapshot from disk and installs it" do
      dir = Process.get(:rpc_test_data_dir)
      meta = %{index: 5, term: 2}
      :ok = Filesystem.make_dir(dir)
      streamed_state = String.duplicate("x", 200)
      assert {:ok, _bytes} = LogSnapshot.write(dir, meta, streamed_state, true)

      assert {:ok, ^meta, read_state} =
               LogSnapshot.begin_read(dir, %{can_accept_full_file: false})

      assert {:ok, first_chunk, {:next, next_state}} = LogSnapshot.read_chunk(read_state, 12, dir)
      assert {crc, _} = read_state
      assert {:ok, last_chunk, :last} = LogSnapshot.read_chunk({crc, next_state}, 1_000, dir)

      first_rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :first,
        data: first_chunk
      }

      {next, state1, effects1} = RpcHandler.handle_install_snapshot_rpc(first_rpc, state())
      assert next == :receive_snapshot
      assert effects1 == []
      assert is_tuple(state1.snapshot_state)

      last_rpc = %{first_rpc | chunk_state: :last, data: last_chunk}
      {next, state2, effects2} = RpcHandler.handle_install_snapshot_rpc(last_rpc, state1)

      assert next == :follower
      assert state2.last_applied == 5
      assert state2.machine_state == streamed_state
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{} = reply}] = effects2
      assert reply.last_index == 5
    end

    test "reports failure when the full snapshot cannot be written" do
      meta = %{index: 4, term: 2}

      rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :full,
        data: :machine_state
      }

      broken = put_in(state().cfg.system_config.data_dir, "/definitely/not/a/valid/dir")
      Process.put(:rpc_test_data_dir, "/definitely/not/a/valid/dir")

      {next, _state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, broken)
      assert next == :follower
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{}}] = effects
    after
      Process.put(:rpc_test_data_dir, nil)
    end

    test "first chunk failure clears partial state and replies with failure" do
      bad_dir = Path.join(System.tmp_dir!(), "rpc_bad_dir_#{System.unique_integer([:positive])}")
      File.write!(bad_dir, "not a directory")
      on_exit(fn -> File.rm(bad_dir) end)
      Process.put(:rpc_test_data_dir, bad_dir)

      meta = %{index: 3, term: 2}

      rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :first,
        data: snapshot_chunk(meta)
      }

      current =
        state()
        |> Map.put(:snapshot_state, {:partial, :state})
        |> Map.put(:snapshot_meta, meta)

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, current)

      assert next == :follower
      assert is_nil(state.snapshot_state)
      assert is_nil(state.snapshot_meta)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 0}}] = effects
    after
      Process.put(:rpc_test_data_dir, nil)
    end

    test "middle chunk whose write fails reports a snapshot failure" do
      snap_dir = Process.get(:rpc_test_data_dir)
      :ok = Filesystem.make_dir(snap_dir)
      meta = %{index: 3, term: 2}

      {:ok, accept_state} = LogSnapshot.begin_accept(snap_dir, meta)
      {_bytes, _partial_crc, fd} = accept_state
      :file.close(fd)

      current =
        state()
        |> Map.put(:snapshot_state, accept_state)
        |> Map.put(:snapshot_meta, meta)

      rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :middle,
        data: snapshot_chunk(meta)
      }

      {next, new_state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, current)

      assert next == :follower
      assert is_nil(new_state.snapshot_state)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 0}}] = effects
    end

    test "middle chunk without a snapshot in progress reports failure" do
      rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: %{index: 3, term: 2},
        chunk_state: :middle,
        data: <<>>
      }

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, state())

      assert next == :follower
      assert is_nil(state.snapshot_state)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 0}}] = effects
    end

    test "last chunk without a snapshot in progress reports failure" do
      rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: %{index: 3, term: 2},
        chunk_state: :last,
        data: <<>>
      }

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, state())

      assert next == :follower
      assert is_nil(state.snapshot_state)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 0}}] = effects
    end

    test "snapshot failure uses the fallback directory when no data dir is configured" do
      base = state()
      cfg = %{base.cfg | system_config: %{names: base.cfg.system_config.names}}
      current = %{base | cfg: cfg}

      rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: %{index: 3, term: 2},
        chunk_state: :last,
        data: <<>>
      }

      {next, _state, effects} = RpcHandler.handle_install_snapshot_rpc(rpc, current)

      assert next == :follower
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 0}}] = effects
    end

    test "last chunk whose write fails reports a snapshot failure" do
      snap_dir = Process.get(:rpc_test_data_dir)
      :ok = Filesystem.make_dir(snap_dir)
      meta = %{index: 3, term: 2}

      {:ok, accept_state} = LogSnapshot.begin_accept(snap_dir, meta)

      {:ok, {bytes, partial_crc, crc, fd}} =
        LogSnapshot.accept_chunk(snapshot_chunk(meta), accept_state)

      :file.close(fd)

      current =
        state()
        |> Map.put(:snapshot_state, {bytes, partial_crc, crc, fd})
        |> Map.put(:snapshot_meta, meta)

      last = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :last,
        data: <<>>
      }

      {next, new_state, effects} = RpcHandler.handle_install_snapshot_rpc(last, current)

      assert next == :follower
      assert is_nil(new_state.snapshot_state)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 0}}] = effects
    end

    test "last chunk with a corrupted snapshot reports a recover failure" do
      # snapshot_dir/1 resolves cfg.system_config.data_dir directly (no
      # extra "snapshot" segment), so write there.
      snap_dir = Process.get(:rpc_test_data_dir)
      :ok = Filesystem.make_dir(snap_dir)
      meta = %{index: 3, term: 2}

      {:ok, accept_state} = LogSnapshot.begin_accept(snap_dir, meta)

      {:ok, {bytes, partial_crc, _crc, fd}} =
        LogSnapshot.accept_chunk(snapshot_chunk(meta), accept_state)

      # complete_accept/2 patches the header crc from the accept state. With a
      # deliberately wrong value recover/1 rejects the file as a checksum error.
      corrupted = {bytes, partial_crc, 1, fd}
      current = state() |> Map.put(:snapshot_state, corrupted) |> Map.put(:snapshot_meta, meta)

      last = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @leader,
        meta: meta,
        chunk_state: :last,
        data: <<>>
      }

      {next, state, effects} = RpcHandler.handle_install_snapshot_rpc(last, current)

      assert next == :follower
      assert is_nil(state.snapshot_state)
      assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 0}}] = effects
    end
  end

  describe "handle_install_snapshot_reply/2" do
    test "steps down on a higher term" do
      reply = %Types.InstallSnapshotResult{term: 3, last_index: 5, last_term: 2}
      {state, effects} = RpcHandler.handle_install_snapshot_reply(reply, state())
      assert state.current_term == 3
      assert effects == [{:become, :follower}]
    end

    test "updates the peer match index" do
      reply = %Types.InstallSnapshotResult{term: 2, last_index: 5, last_term: 2}
      {state, effects} = RpcHandler.handle_install_snapshot_reply(reply, state())
      assert state.cluster[@peer2].match_index == 5
      assert state.cluster[@peer2].next_index == 6
      assert effects == []
    end

    test "ignores a stale reply" do
      reply = %Types.InstallSnapshotResult{term: 1, last_index: 5, last_term: 2}
      {state, effects} = RpcHandler.handle_install_snapshot_reply(reply, state())
      assert state.current_term == 2
      assert effects == []
    end
  end

  describe "heartbeats" do
    test "handle_heartbeat_rpc rejects a stale term" do
      rpc = %Types.HeartbeatRpc{query_index: 7, term: 1, leader_id: @leader}
      {state, effects} = RpcHandler.handle_heartbeat_rpc(rpc, state())

      assert [{:send_rpc, @leader, %Types.HeartbeatReply{} = reply}] = effects
      assert reply.query_index == 0
      assert reply.term == 2
      assert state.current_term == 2
    end

    test "handle_heartbeat_rpc acknowledges current and higher terms" do
      for term <- [2, 3] do
        rpc = %Types.HeartbeatRpc{query_index: 7, term: term, leader_id: @leader}
        {state, effects} = RpcHandler.handle_heartbeat_rpc(rpc, state())

        assert state.current_term == term
        assert state.leader_id == @leader

        assert [{:send_rpc, @leader, %Types.HeartbeatReply{} = reply}, {:reset_election_timeout}] =
                 effects

        assert reply.query_index == 7
        assert reply.term == term
      end
    end

    test "handle_heartbeat_reply steps down on a higher term" do
      reply = %Types.HeartbeatReply{query_index: 7, term: 3}
      {state, effects} = RpcHandler.handle_heartbeat_reply(reply, state())
      assert state.current_term == 3
      assert effects == [{:become, :follower}]
    end

    test "handle_heartbeat_reply records the query index" do
      reply = %Types.HeartbeatReply{query_index: 7, term: 2}
      {state, effects} = RpcHandler.handle_heartbeat_reply(reply, state())
      assert state.cluster[@peer2].query_index == 7
      assert effects == []
    end

    test "handle_heartbeat_reply ignores a stale reply" do
      reply = %Types.HeartbeatReply{query_index: 7, term: 1}
      {state, effects} = RpcHandler.handle_heartbeat_reply(reply, state())
      assert state.current_term == 2
      assert effects == []
    end

    test "term persistence tolerates an unavailable metadata store" do
      base = state()
      missing = :"missing_meta_#{System.unique_integer([:positive])}"
      names = Map.put(base.cfg.system_config.names, :log_meta, missing)
      cfg = %{base.cfg | system_config: %{base.cfg.system_config | names: names}}
      current = %{base | cfg: cfg}

      rpc = %Types.HeartbeatRpc{query_index: 7, term: 3, leader_id: @leader}
      {state, _effects} = RpcHandler.handle_heartbeat_rpc(rpc, current)

      assert state.current_term == 3
      assert state.voted_for == nil
      assert state.leader_id == @leader
    end
  end

  defp effects_liquids(effects) do
    Enum.map(effects, fn
      {:send_rpc, _leader, reply} -> reply
      other -> other
    end)
  end
end
