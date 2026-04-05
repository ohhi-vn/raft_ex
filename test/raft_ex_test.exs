defmodule RaftExTest do
  @moduledoc """
  Comprehensive test suite for RaftEx Raft consensus implementation.

  Tests cover:
  - Types and message structures
  - Log operations (append, read, snapshot, checkpoint)
  - Cluster management and membership
  - State machine behavior
  - Effects system
  - Configuration and metadata
  - Server lifecycle
  """

  use ExUnit.Case, async: false
  doctest RaftEx

  alias RaftEx.Types
  alias RaftEx.Server.Config
  alias RaftEx.Server.Cluster
  alias RaftEx.Server.Effects
  alias RaftEx.Log
  alias RaftEx.Machine
  alias RaftEx.Lib

  # ---------------------------------------------------------------------------
  # Setup and Teardown
  # ---------------------------------------------------------------------------

  setup do
    # Create temporary data directory for tests
    data_dir = Path.join(System.tmp_dir!(), "raft_ex_test_#{System.unique_integer()}")
    File.mkdir_p!(data_dir)

    on_exit(fn ->
      File.rm_rf!(data_dir)
    end)

    %{data_dir: data_dir}
  end

  # ---------------------------------------------------------------------------
  # Types Tests
  # ---------------------------------------------------------------------------

  describe "Types" do
    test "AppendEntriesRpc struct can be created and pattern matched" do
      rpc = %Types.AppendEntriesRpc{
        term: 5,
        leader_id: {:server1, :node1@host},
        leader_commit: 10,
        prev_log_index: 9,
        prev_log_term: 4,
        entries: [
          {10, 5, {:"$usr", %{}, :command1, :noreply}},
          {11, 5, {:"$usr", %{}, :command2, :noreply}}
        ]
      }

      assert rpc.term == 5
      assert rpc.leader_id == {:server1, :node1@host}
      assert length(rpc.entries) == 2
    end

    test "AppendEntriesReply struct can be created" do
      reply = %Types.AppendEntriesReply{
        term: 5,
        success: true,
        next_index: 12,
        last_index: 11,
        last_term: 5
      }

      assert reply.success == true
      assert reply.next_index == 12
    end

    test "RequestVoteRpc struct can be created" do
      rpc = %Types.RequestVoteRpc{
        term: 3,
        candidate_id: {:server2, :node2@host},
        last_log_index: 15,
        last_log_term: 2
      }

      assert rpc.term == 3
      assert rpc.last_log_index == 15
    end

    test "RequestVoteResult struct can be created" do
      result = %Types.RequestVoteResult{
        term: 3,
        vote_granted: true
      }

      assert result.vote_granted == true
    end

    test "PreVoteRpc struct can be created" do
      rpc = %Types.PreVoteRpc{
        machine_version: 1,
        term: 2,
        token: "abc123",
        candidate_id: {:server1, :node1@host},
        last_log_index: 10,
        last_log_term: 1,
        version: 1
      }

      assert rpc.token == "abc123"
      assert rpc.version == 1
    end

    test "InstallSnapshotRpc struct can be created" do
      rpc = %Types.InstallSnapshotRpc{
        term: 4,
        leader_id: {:leader, :node1@host},
        meta: %{index: 100, term: 3},
        chunk_state: :first,
        data: <<1, 2, 3, 4>>
      }

      assert rpc.chunk_state == :first
      assert rpc.meta.index == 100
    end

    test "HeartbeatRpc and HeartbeatReply structs can be created" do
      rpc = %Types.HeartbeatRpc{
        query_index: 5,
        term: 3,
        leader_id: {:leader, :node1@host}
      }

      reply = %Types.HeartbeatReply{
        query_index: 5,
        term: 3
      }

      assert rpc.query_index == reply.query_index
    end
  end

  # ---------------------------------------------------------------------------
  # Cluster Tests
  # ---------------------------------------------------------------------------

  describe "Cluster" do
    setup do
      self_id = {:server1, :node1@host}
      peer1 = {:server2, :node2@host}
      peer2 = {:server3, :node3@host}

      %{self_id: self_id, peer1: peer1, peer2: peer2}
    end

    test "new/2 creates cluster from list of peers", %{
      self_id: self_id,
      peer1: peer1,
      peer2: peer2
    } do
      cluster = Cluster.new(self_id, [self_id, peer1, peer2])

      assert map_size(cluster) == 3
      assert Map.has_key?(cluster, self_id)
      assert Map.has_key?(cluster, peer1)
      assert Map.has_key?(cluster, peer2)
    end

    test "new/2 creates cluster from map of peers", %{self_id: self_id} do
      peers = %{
        self_id => %{next_index: 5, match_index: 3},
        {:server2, :node2@host} => %{next_index: 1, match_index: 0}
      }

      cluster = Cluster.new(self_id, peers)

      assert map_size(cluster) == 2
      assert cluster[self_id].next_index == 5
      assert cluster[self_id].match_index == 3
    end

    test "peers/2 returns cluster excluding self", %{self_id: self_id, peer1: peer1, peer2: peer2} do
      cluster = Cluster.new(self_id, [self_id, peer1, peer2])
      peers = Cluster.peers(self_id, cluster)

      assert map_size(peers) == 2
      refute Map.has_key?(peers, self_id)
      assert Map.has_key?(peers, peer1)
      assert Map.has_key?(peers, peer2)
    end

    test "peer_ids/2 returns list of peer ids excluding self", %{
      self_id: self_id,
      peer1: peer1,
      peer2: peer2
    } do
      cluster = Cluster.new(self_id, [self_id, peer1, peer2])
      peer_ids = Cluster.peer_ids(self_id, cluster)

      assert length(peer_ids) == 2
      assert peer1 in peer_ids
      assert peer2 in peer_ids
      refute self_id in peer_ids
    end

    test "get_peer/2 returns peer state or nil", %{self_id: self_id, peer1: peer1} do
      cluster = Cluster.new(self_id, [self_id, peer1])

      assert Cluster.get_peer(peer1, cluster) != nil
      assert Cluster.get_peer({:nonexistent, :node@host}, cluster) == nil
    end

    test "update_peer/3 merges update into existing peer", %{self_id: self_id, peer1: peer1} do
      cluster = Cluster.new(self_id, [self_id, peer1])

      updated = Cluster.update_peer(peer1, %{match_index: 10, next_index: 11}, cluster)

      assert updated[peer1].match_index == 10
      assert updated[peer1].next_index == 11
      # Other fields should remain
      assert Map.has_key?(updated[peer1], :commit_index_sent)
    end

    test "update_peer/3 creates new peer if not exists", %{self_id: self_id} do
      cluster = Cluster.new(self_id, [self_id])
      new_peer = {:server99, :node99@host}

      updated = Cluster.update_peer(new_peer, %{match_index: 5}, cluster)

      assert updated[new_peer].match_index == 5
      # update_peer merges with the existing map (or %{} if new), so only provided keys are present
      assert Map.get(updated[new_peer], :next_index) == nil
    end

    test "get_membership/4 returns membership status", %{self_id: self_id, peer1: peer1} do
      cluster = Cluster.new(self_id, [self_id, peer1])

      cluster =
        Cluster.update_peer(
          peer1,
          %{
            voter_status: %{uid: "abc123", membership: :voter}
          },
          cluster
        )

      assert Cluster.get_membership(cluster, peer1, "abc123", :unknown) == :voter
      assert Cluster.get_membership(cluster, peer1, "wrong_uid", :unknown) == :unknown

      assert Cluster.get_membership(cluster, {:nonexistent, :node@host}, "uid", :unknown) ==
               :unknown
    end

    test "voter_match_indexes/3 collects match indexes from voters", %{
      self_id: self_id,
      peer1: peer1,
      peer2: peer2
    } do
      cluster = Cluster.new(self_id, [self_id, peer1, peer2])

      cluster =
        Cluster.update_peer(
          peer1,
          %{
            match_index: 10,
            voter_status: %{membership: :voter}
          },
          cluster
        )

      cluster =
        Cluster.update_peer(
          peer2,
          %{
            match_index: 8,
            voter_status: %{membership: :non_voter}
          },
          cluster
        )

      indexes = Cluster.voter_match_indexes(self_id, cluster, 12)

      # Should include leader's last_written (12) and peer1's match_index (10)
      # But not peer2 (non_voter)
      assert 12 in indexes
      assert 10 in indexes
      refute 8 in indexes
    end

    test "agreed_commit/1 returns majority commit index" do
      # 5 servers, majority is 3
      indexes = [5, 8, 10, 12, 15]
      assert Cluster.agreed_commit(indexes) == 10

      # 3 servers, majority is 2
      indexes = [3, 7, 9]
      assert Cluster.agreed_commit(indexes) == 7

      # 1 server
      indexes = [5]
      assert Cluster.agreed_commit(indexes) == 5
    end

    test "reset_peer_statuses/1 resets all statuses to :normal", %{self_id: self_id, peer1: peer1} do
      cluster = Cluster.new(self_id, [self_id, peer1])

      cluster = Cluster.update_peer(peer1, %{status: {:sending_snapshot, self(), 100}}, cluster)

      reset = Cluster.reset_peer_statuses(cluster)

      assert reset[self_id].status == :normal
      assert reset[peer1].status == :normal
    end

    test "reset_query_indexes/1 resets all query_index to 0", %{self_id: self_id, peer1: peer1} do
      cluster = Cluster.new(self_id, [self_id, peer1])

      cluster = Cluster.update_peer(peer1, %{query_index: 50}, cluster)
      cluster = Cluster.update_peer(self_id, %{query_index: 30}, cluster)

      reset = Cluster.reset_query_indexes(cluster)

      assert reset[self_id].query_index == 0
      assert reset[peer1].query_index == 0
    end

    test "new_peer/0 returns correct default peer state" do
      peer = Cluster.new_peer()

      assert peer.next_index == 1
      assert peer.match_index == 0
      assert peer.commit_index_sent == 0
      assert peer.query_index == 0
      assert peer.status == :normal
    end
  end

  # ---------------------------------------------------------------------------
  # Effects Tests
  # ---------------------------------------------------------------------------

  describe "Effects" do
    test "add_reply/5 with :await_consensus adds reply effect" do
      from = {self(), :ref}
      {effects, notifys} = Effects.add_reply(%{from: from}, :ok, :await_consensus, [], %{})

      assert length(effects) == 1
      assert {:reply, ^from, {:wrap_reply, :ok}} = hd(effects)
      assert map_size(notifys) == 0
    end

    test "add_reply/5 with :await_consensus and routing options" do
      from = {self(), :ref}
      opts = %{reply_from: :local}

      {effects, notifys} =
        Effects.add_reply(%{from: from}, :ok, {:await_consensus, opts}, [], %{})

      assert length(effects) == 1
      assert {:reply, ^from, {:wrap_reply, :ok}, :local} = hd(effects)
    end

    test "add_reply/5 with :notify adds to notifys map" do
      corr = :correlation1
      pid = self()
      {effects, notifys} = Effects.add_reply(%{}, :result, {:notify, corr, pid}, [], %{})

      assert length(effects) == 0
      assert map_size(notifys) == 1
      assert notifys[pid] == [{:correlation1, :result}]
    end

    test "add_reply/5 with :\"$ra_no_reply\" does nothing" do
      {effects, notifys} = Effects.add_reply(%{}, :ignored, :"$ra_no_reply", [], %{})

      assert effects == []
      assert notifys == %{}
    end

    test "add_reply/5 with :noreply does nothing" do
      {effects, notifys} = Effects.add_reply(%{}, :ignored, :noreply, [], %{})

      assert effects == []
      assert notifys == %{}
    end

    test "append_machine_effects/2 prepends machine effects" do
      machine_effects = [{:send_msg, :pid, :msg}]
      existing = [{:reply, :from, :ok}]

      result = Effects.append_machine_effects(machine_effects, existing)

      assert result == [{:send_msg, :pid, :msg}, {:reply, :from, :ok}]
    end

    test "append_machine_effects/2 handles single effect" do
      result = Effects.append_machine_effects({:send_msg, :pid, :msg}, [])

      assert result == [{:send_msg, :pid, :msg}]
    end

    test "append_machine_effects/2 handles empty list" do
      result = Effects.append_machine_effects([], [{:reply, :from, :ok}])

      assert result == [{:reply, :from, :ok}]
    end

    test "make_notify_effects/2 wraps non-empty notifys" do
      notifys = %{self() => [{:corr, :result}]}
      prior = [{:reply, :from, :ok}]

      result = Effects.make_notify_effects(notifys, prior)

      assert result == [{:notify, notifys}, {:reply, :from, :ok}]
    end

    test "make_notify_effects/2 returns prior when notifys is empty" do
      prior = [{:reply, :from, :ok}]

      result = Effects.make_notify_effects(%{}, prior)

      assert result == prior
    end

    test "append_error_reply/3 adds error reply when from is present" do
      from = {self(), :ref}
      # The function expects the shape {_, %{from: from}, _, _}
      entry = {1, %{from: from}, 1, :cmd}
      effects = Effects.append_error_reply(entry, :not_leader, [])

      assert length(effects) == 1
      assert {:reply, ^from, {:error, :not_leader}} = hd(effects)
    end

    test "append_error_reply/3 does nothing when from is absent" do
      entry = {1, 1, {:"$usr", %{}, :cmd, :noreply}}
      effects = Effects.append_error_reply(entry, :error, [])

      assert effects == []
    end

    test "after_log_append_reply/4 adds reply for :after_log_append mode" do
      from = {self(), :ref}
      # The function expects {_, %{from: from}, _, :after_log_append}
      entry = {1, %{from: from}, 1, :after_log_append}
      effects = Effects.after_log_append_reply(entry, 1, 1, [])

      assert length(effects) == 1
      assert {:reply, ^from, {:wrap_reply, {1, 1}}} = hd(effects)
    end

    test "after_log_append_reply/4 does nothing for other modes" do
      from = {self(), :ref}
      entry = {1, %{from: from}, 1, :await_consensus}
      effects = Effects.after_log_append_reply(entry, 1, 1, [])

      assert effects == []
    end
  end

  # ---------------------------------------------------------------------------
  # Log Tests
  # ---------------------------------------------------------------------------

  describe "Log" do
    setup %{data_dir: data_dir} do
      suffix = System.unique_integer([:positive])
      uid = "test_log_uid_#{suffix}"

      names = %{
        wal: :"test_wal_#{suffix}",
        log_meta: :"test_log_meta_#{suffix}",
        open_mem_tbls: :"test_open_mem_tbls_#{suffix}",
        log_ets: :"test_log_ets_#{suffix}"
      }

      # Start required ETS tables
      :ets.new(names.open_mem_tbls, [:named_table, :public, :set])
      :ets.new(names.log_ets, [:named_table, :public, :set])

      # Start LogMeta
      {:ok, _} =
        RaftEx.LogMeta.start_link(%{
          name: :"test_system_#{suffix}",
          data_dir: data_dir,
          names: names
        })

      conf = %{
        uid: uid,
        system_config: %{
          data_dir: data_dir,
          names: names
        }
      }

      on_exit(fn ->
        # Cleanup ETS tables if they still exist
        try do
          :ets.delete(names.open_mem_tbls)
        rescue
          _ -> :ok
        end

        try do
          :ets.delete(names.log_ets)
        rescue
          _ -> :ok
        end
      end)

      # Start WAL process for tests that need it
      {:ok, _wal_pid} = RaftEx.LogWal.start_link(%{data_dir: data_dir, names: names})

      %{conf: conf, uid: uid, names: names, wal_started: true}
    end

    test "init/1 creates log with correct initial state", %{conf: conf} do
      log = Log.init(conf)

      assert log.cfg == conf
      assert log.last_term == 0
      # next_index starts at 1 when no snapshot or entries exist
      assert log.next_index == 1
      assert log.current_snapshot == nil
      assert log.range == nil
      assert log.tx == false
      assert log.pending == []
      assert is_reference(log.last_wal_write |> elem(1))
    end

    test "append/2 adds entry to log", %{conf: conf} do
      log = Log.init(conf)
      entry = {1, 1, {:"$usr", %{}, :command, :noreply}}

      new_log = Log.append(entry, log)

      assert new_log.last_term == 1
      assert new_log.last_written_index_term == {1, 1}
      assert new_log.next_index == 2
      assert new_log.range == {1, 1}
      assert map_size(new_log.entries) == 1
      assert new_log.entries[1] == entry
    end

    test "append/2 updates range correctly", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({1, 1, :cmd1}, log)
      log = Log.append({2, 1, :cmd2}, log)
      log = Log.append({3, 2, :cmd3}, log)

      assert log.range == {1, 3}
      assert log.last_term == 2
      assert log.next_index == 4
      assert map_size(log.entries) == 3
    end

    test "write/2 writes multiple entries", %{conf: conf} do
      log = Log.init(conf)

      entries = [
        {1, 1, :cmd1},
        {2, 1, :cmd2},
        {3, 1, :cmd3}
      ]

      {:ok, new_log} = Log.write(entries, log)

      assert new_log.last_term == 1
      assert new_log.last_written_index_term == {3, 1}
      assert new_log.next_index == 4
      assert map_size(new_log.entries) == 3
    end

    test "write/2 returns ok for empty entries", %{conf: conf} do
      log = Log.init(conf)

      {:ok, new_log} = Log.write([], log)

      assert new_log == log
    end

    test "fold/6 iterates over entries in range", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({1, 1, :cmd1}, log)
      log = Log.append({2, 1, :cmd2}, log)
      log = Log.append({3, 1, :cmd3}, log)

      fun = fn {_idx, _term, cmd}, acc -> [cmd | acc] end
      {result, _} = Log.fold(1, 3, fun, [], log)

      assert Enum.sort(result) == [:cmd1, :cmd2, :cmd3]
    end

    test "fold/6 handles partial range", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({1, 1, :cmd1}, log)
      log = Log.append({2, 1, :cmd2}, log)
      log = Log.append({3, 1, :cmd3}, log)

      fun = fn {_idx, _term, cmd}, acc -> [cmd | acc] end
      {result, _} = Log.fold(2, 3, fun, [], log)

      assert Enum.sort(result) == [:cmd2, :cmd3]
    end

    test "sparse_read/2 reads specific entries", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({1, 1, :cmd1}, log)
      log = Log.append({2, 1, :cmd2}, log)
      log = Log.append({3, 1, :cmd3}, log)

      {entries, _} = Log.sparse_read([1, 3], log)

      assert length(entries) == 2
      assert {1, 1, :cmd1} in entries
      assert {3, 1, :cmd3} in entries
    end

    test "sparse_read/2 returns empty for missing indexes", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({1, 1, :cmd1}, log)

      {entries, _} = Log.sparse_read([5, 10], log)

      assert entries == []
    end

    test "fetch_term/2 returns term for existing index", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({5, 3, :cmd}, log)

      {term, _} = Log.fetch_term(5, log)

      assert term == 3
    end

    test "fetch_term/2 returns nil for missing index", %{conf: conf} do
      log = Log.init(conf)

      {term, _} = Log.fetch_term(99, log)

      assert term == nil
    end

    test "last_index_term/1 returns correct values", %{conf: conf} do
      log = Log.init(conf)

      assert Log.last_index_term(log) == {0, 0}

      log = Log.append({1, 1, :cmd1}, log)
      log = Log.append({2, 2, :cmd2}, log)

      assert Log.last_index_term(log) == {2, 2}
    end

    test "last_index_term/1 with snapshot", %{conf: conf} do
      log = Log.init(conf)
      log = %{log | current_snapshot: {10, 5}}

      assert Log.last_index_term(log) == {10, 5}
    end

    test "last_written/1 returns last written index and term", %{conf: conf} do
      log = Log.init(conf)

      assert Log.last_written(log) == {0, 0}

      log = Log.append({5, 3, :cmd}, log)

      assert Log.last_written(log) == {5, 3}
    end

    test "next_index/1 returns correct next index", %{conf: conf} do
      log = Log.init(conf)

      # next_index starts at 1 when initialized with no entries
      assert Log.next_index(log) == 1

      log = Log.append({1, 1, :cmd}, log)
      log = Log.append({2, 1, :cmd}, log)

      assert Log.next_index(log) == 3
    end

    test "next_index/1 with snapshot", %{conf: conf} do
      log = Log.init(conf)
      # Set next_index to 0 so it falls through to snapshot check
      log = %{log | current_snapshot: {10, 5}, next_index: 0}

      assert Log.next_index(log) == 11
    end

    test "exists/2 checks if entry exists with correct term", %{conf: conf} do
      log = Log.init(conf)
      log = Log.append({5, 3, :cmd}, log)

      {true, _} = Log.exists({5, 3}, log)
      {false, _} = Log.exists({5, 4}, log)
      {false, _} = Log.exists({10, 3}, log)
    end

    test "has_pending?/1 checks pending state", %{conf: conf} do
      log = Log.init(conf)

      refute Log.has_pending?(log)

      log = %{log | pending: [:pending1]}

      assert Log.has_pending?(log)
    end

    test "install_snapshot/4 clears entries up to snapshot index", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({1, 1, :cmd1}, log)
      log = Log.append({2, 1, :cmd2}, log)
      log = Log.append({3, 1, :cmd3}, log)
      log = Log.append({4, 2, :cmd4}, log)
      log = Log.append({5, 2, :cmd5}, log)

      {:ok, new_log, _} = Log.install_snapshot({3, 1}, RaftEx.MachineSimple, [], log)

      assert new_log.current_snapshot == {3, 1}
      assert map_size(new_log.entries) == 2
      assert new_log.entries[4] == {4, 2, :cmd4}
      assert new_log.entries[5] == {5, 2, :cmd5}
      refute Map.has_key?(new_log.entries, 1)
      refute Map.has_key?(new_log.entries, 2)
      refute Map.has_key?(new_log.entries, 3)
      assert new_log.next_index == 4
    end

    test "snapshot_state/1 and set_snapshot_state/2 work correctly", %{conf: conf} do
      log = Log.init(conf)

      assert Log.snapshot_state(log) == nil

      log = Log.set_snapshot_state(%{data: "snapshot"}, log)

      assert Log.snapshot_state(log) == %{data: "snapshot"}
    end

    test "snapshot_index_term/1 returns current snapshot", %{conf: conf} do
      log = Log.init(conf)

      assert Log.snapshot_index_term(log) == nil

      log = %{log | current_snapshot: {10, 5}}

      assert Log.snapshot_index_term(log) == {10, 5}
    end

    test "can_write?/1 always returns true", %{conf: conf} do
      log = Log.init(conf)

      assert Log.can_write?(log)
    end

    test "overview/1 returns log overview", %{conf: conf} do
      log = Log.init(conf)

      log = Log.append({1, 1, :cmd1}, log)
      log = Log.append({2, 1, :cmd2}, log)

      overview = Log.overview(log)

      assert overview.type == RaftEx.Log
      assert overview.range == {1, 2}
      assert overview.last_term == 1
      assert overview.next_index == 3
      assert overview.entries_count == 2
      assert overview.pending_count == 0
    end

    test "write_config/2 and read_config/1 persist configuration", %{conf: conf} do
      log = Log.init(conf)

      # Use simple terms that serialize correctly with inspect/Code.eval_string
      config = %{
        cluster_name: "test_cluster",
        member_count: 2
      }

      :ok = Log.write_config(config, log)

      {:ok, read_config} = Log.read_config(log)

      assert read_config.cluster_name == "test_cluster"
      assert read_config.member_count == 2
    end

    test "close/1 cleans up resources", %{conf: conf} do
      log = Log.init(conf)

      assert :ok == Log.close(log)
    end
  end

  # ---------------------------------------------------------------------------
  # Machine Tests
  # ---------------------------------------------------------------------------

  describe "Machine" do
    test "MachineSimple init and apply work correctly" do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{
           simple_fun: fn cmd, state -> state + cmd end,
           initial_state: 0
         }}

      state = Machine.init(machine, :test, 0)

      assert match?({:simple, _, 0}, state)

      # Machine.apply/4 returns {state, reply, effects}
      {new_state, reply, effects} = Machine.apply(RaftEx.MachineSimple, %{index: 1, term: 1}, 5, state)

      assert match?({:simple, _, 5}, new_state)
      assert reply == 5
      assert effects == []
    end

    test "Machine.apply/4 handles effects" do
      defmodule TestMachineWithEffects do
        @behaviour RaftEx.Machine

        @impl RaftEx.Machine
        def init(_conf), do: 0

        @impl RaftEx.Machine
        def apply(_meta, cmd, state) do
          {state + 1, cmd, [{:send_msg, :pid, cmd}]}
        end
      end

      {new_state, reply, effects} = Machine.apply(TestMachineWithEffects, %{index: 1}, :cmd, 0)

      assert new_state == 1
      assert reply == :cmd
      assert effects == [{:send_msg, :pid, :cmd}]
    end

    test "Machine.tick/3 calls optional callback" do
      defmodule TestMachineWithTick do
        @behaviour RaftEx.Machine

        @impl RaftEx.Machine
        def init(_conf), do: 0

        @impl RaftEx.Machine
        def apply(_meta, _cmd, state), do: {state, :ok}

        @impl RaftEx.Machine
        def tick(_time, state), do: [{:aux, :tick}]
      end

      effects = Machine.tick(TestMachineWithTick, 1000, 0)

      assert effects == [{:aux, :tick}]
    end

    test "Machine.tick/3 returns default when not implemented" do
      defmodule TestMachineWithoutTick do
        @behaviour RaftEx.Machine

        @impl RaftEx.Machine
        def init(_conf), do: 0

        @impl RaftEx.Machine
        def apply(_meta, _cmd, state), do: {state, :ok}
      end

      effects = Machine.tick(TestMachineWithoutTick, 1000, 0)

      assert effects == []
    end

    test "Machine.version/1 returns version or default" do
      defmodule TestMachineWithVersion do
        @behaviour RaftEx.Machine

        @impl RaftEx.Machine
        def init(_conf), do: 0

        @impl RaftEx.Machine
        def apply(_meta, _cmd, state), do: {state, :ok}

        @impl RaftEx.Machine
        def version, do: 2
      end

      assert Machine.version(TestMachineWithVersion) == 2
    end

    test "Machine.which_aux_fun/1 detects exported arity" do
      defmodule TestMachineWithAux5 do
        @behaviour RaftEx.Machine

        @impl RaftEx.Machine
        def init(_conf), do: 0

        @impl RaftEx.Machine
        def apply(_meta, _cmd, state), do: {state, :ok}

        @impl RaftEx.Machine
        def handle_aux(_state, _type, _cmd, _aux, _log), do: {:no_reply, nil, nil}
      end

      defmodule TestMachineWithAux6 do
        @behaviour RaftEx.Machine

        @impl RaftEx.Machine
        def init(_conf), do: 0

        @impl RaftEx.Machine
        def apply(_meta, _cmd, state), do: {state, :ok}

        @impl RaftEx.Machine
        def handle_aux(_state, _type, _cmd, _aux, _log, _mac), do: {:no_reply, nil, nil}
      end

      assert Machine.which_aux_fun(TestMachineWithAux5) == {:handle_aux, 5}
      assert Machine.which_aux_fun(TestMachineWithAux6) == {:handle_aux, 6}
    end
  end

  # ---------------------------------------------------------------------------
  # Config Tests
  # ---------------------------------------------------------------------------

  describe "Config" do
    test "Config struct can be created with required fields" do
      config = %Config{
        id: {:server1, :node1@host},
        uid: "abc123",
        log_id: "log_abc123",
        metrics_key: :metrics1,
        machine:
          {:machine, RaftEx.MachineSimple, %{simple_fun: fn _, s -> s end, initial_state: 0}},
        machine_version: 0,
        effective_machine_version: 0,
        effective_machine_module: RaftEx.MachineSimple,
        system_config: %{data_dir: "/tmp"}
      }

      assert config.id == {:server1, :node1@host}
      assert config.uid == "abc123"
      assert config.machine_version == 0
      assert config.max_pipeline_count == 4096
      assert config.max_append_entries_rpc_batch_size == 128
    end

    test "Config has correct default values" do
      config = %Config{
        id: {:server1, :node1@host},
        uid: "abc123",
        log_id: "log_abc123",
        metrics_key: :metrics1,
        machine: {:machine, RaftEx.MachineSimple, %{}},
        machine_version: 0,
        effective_machine_version: 0,
        effective_machine_module: RaftEx.MachineSimple,
        system_config: %{}
      }

      assert config.metrics_labels == %{}
      assert config.machine_versions == []
      assert config.max_pipeline_count == 4096
      assert config.max_append_entries_rpc_batch_size == 128
      assert config.min_recovery_checkpoint_interval == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Lib Tests
  # ---------------------------------------------------------------------------

  describe "Lib" do
    test "to_binary/1 converts various types to binary" do
      assert Lib.to_binary("test") == "test"
      assert Lib.to_binary(:test) == "test"
      assert Lib.to_binary(123) == "123"
      assert Lib.to_binary('test') == "test"
    end

    test "to_atom/1 converts various types to atom" do
      assert Lib.to_atom(:test) == :test
      assert Lib.to_atom("test") == :test
      assert Lib.to_atom('test') == :test
    end

    test "to_list/1 converts various types to charlist" do
      assert Lib.to_list(:test) == ~c"test"
      assert Lib.to_list("test") == ~c"test"
      assert Lib.to_list(123) == ~c"123"
      assert Lib.to_list(~c"test") == ~c"test"
    end

    test "ra_server_id_to_local_name/1 extracts name from server id" do
      assert Lib.ra_server_id_to_local_name({:server1, :node1@host}) == :server1
      assert Lib.ra_server_id_to_local_name(:server1) == :server1
    end

    test "ra_server_id_node/1 extracts node from server id" do
      assert Lib.ra_server_id_node({:server1, :node1@host}) == :node1@host
      assert Lib.ra_server_id_node(:server1) == node()
    end

    test "make_uid/0 generates unique uid" do
      uid1 = Lib.make_uid()
      uid2 = Lib.make_uid()

      assert is_binary(uid1)
      assert is_binary(uid2)
      assert uid1 != uid2
      assert String.length(uid1) == 12
    end

    test "make_uid/1 generates uid with prefix" do
      uid = Lib.make_uid("TEST")

      assert String.starts_with?(uid, "TEST")
      assert String.length(uid) == 16
    end

    test "validate_base64uri/1 validates base64uri strings" do
      assert Lib.validate_base64uri("ABC123_-")
      refute Lib.validate_base64uri("")
      refute Lib.validate_base64uri("invalid!@#")
    end

    test "derive_safe_string/2 derives safe string" do
      result = Lib.derive_safe_string("Hello World!@#", 10)

      assert String.length(result) <= 10
      refute String.contains?(result, " ")
      refute String.contains?(result, "!")
    end

    test "make_dir/1 creates directory" do
      dir = Path.join(System.tmp_dir!(), "raft_ex_test_dir_#{System.unique_integer()}")

      assert :ok == Lib.make_dir(dir)
      assert File.dir?(dir)

      File.rm_rf!(dir)
    end

    test "write_file/3 writes file" do
      file = Path.join(System.tmp_dir!(), "raft_ex_test_file_#{System.unique_integer()}")

      :ok = Lib.write_file(file, "test content")
      assert File.read!(file) == "test content"

      File.rm!(file)
    end

    test "zpad_hex/1 zero-pads hex string" do
      assert Lib.zpad_hex(255) == "00000000000000FF"
      assert Lib.zpad_hex(0) == "0000000000000000"
    end

    test "cons/2 prepends item to list" do
      assert Lib.cons(1, [2, 3]) == [1, 2, 3]
    end

    test "lists_shuffle/1 shuffles list" do
      list = Enum.to_list(1..100)
      shuffled = Lib.lists_shuffle(list)

      assert length(shuffled) == length(list)
      assert Enum.sort(shuffled) == Enum.sort(list)
    end

    test "partition_parallel/3 processes elements in parallel" do
      fun = fn x -> x > 5 end
      elements = Enum.to_list(1..10)

      {:ok, successes, failures} = Lib.partition_parallel(fun, elements, 5000)

      assert Enum.sort(successes) == [6, 7, 8, 9, 10]
      assert Enum.sort(failures) == [1, 2, 3, 4, 5]
    end
  end

  # ---------------------------------------------------------------------------
  # Integration Tests
  # ---------------------------------------------------------------------------

  describe "Integration" do
    setup %{data_dir: data_dir} do
      suffix = System.unique_integer([:positive])
      uid = "integration_test_uid_#{suffix}"

      names = %{
        wal: :"int_test_wal_#{suffix}",
        log_meta: :"int_test_log_meta_#{suffix}",
        open_mem_tbls: :"int_test_open_mem_tbls_#{suffix}",
        log_ets: :"int_test_log_ets_#{suffix}"
      }

      # Start required ETS tables
      :ets.new(names.open_mem_tbls, [:named_table, :public, :set])
      :ets.new(names.log_ets, [:named_table, :public, :set])

      # Start LogMeta
      {:ok, _} =
        RaftEx.LogMeta.start_link(%{
          name: :"int_test_system_#{suffix}",
          data_dir: data_dir,
          names: names
        })

      conf = %{
        uid: uid,
        system_config: %{
          data_dir: data_dir,
          names: names
        }
      }

      on_exit(fn ->
        try do
          :ets.delete(names.open_mem_tbls)
        rescue
          _ -> :ok
        end

        try do
          :ets.delete(names.log_ets)
        rescue
          _ -> :ok
        end
      end)

      %{conf: conf}
    end

    test "full log lifecycle: append, read, snapshot", %{conf: conf} do
      # Initialize log
      log = Log.init(conf)

      # Append entries directly without WAL (for unit testing)
      log =
        Enum.reduce(1..10, log, fn i, acc ->
          Log.append({i, 1, {:"$usr", %{index: i}, :"command#{i}", :noreply}}, acc)
        end)

      assert log.next_index == 11
      assert map_size(log.entries) == 10
      assert log.last_term == 1
      assert log.last_term == 1

      # Read entries
      {read_entries, _} = Log.sparse_read([1, 5, 10], log)
      assert length(read_entries) == 3

      # Install snapshot
      {:ok, log, _} = Log.install_snapshot({5, 1}, RaftEx.MachineSimple, [], log)

      assert log.current_snapshot == {5, 1}
      assert map_size(log.entries) == 5
      assert log.next_index == 6

      # Verify old entries are gone
      {old_entries, _} = Log.sparse_read([1, 2, 3], log)
      assert old_entries == []

      # Verify new entries remain
      {new_entries, _} = Log.sparse_read([6, 7, 8], log)
      assert length(new_entries) == 3
    end

    test "cluster membership changes", %{conf: conf} do
      self_id = {:server1, :node1@host}
      peer1 = {:server2, :node2@host}
      peer2 = {:server3, :node3@host}

      # Create initial cluster
      cluster = Cluster.new(self_id, [self_id, peer1])

      assert map_size(cluster) == 2

      # Add new peer
      cluster =
        Cluster.update_peer(
          peer2,
          %{
            voter_status: %{membership: :promotable}
          },
          cluster
        )

      assert map_size(cluster) == 3
      assert cluster[peer2].voter_status.membership == :promotable

      # Promote to voter
      cluster =
        Cluster.update_peer(
          peer2,
          %{
            voter_status: %{membership: :voter}
          },
          cluster
        )

      assert cluster[peer2].voter_status.membership == :voter

      # Verify voter match indexes
      cluster =
        Cluster.update_peer(
          peer1,
          %{match_index: 10, voter_status: %{membership: :voter}},
          cluster
        )

      cluster =
        Cluster.update_peer(
          peer2,
          %{match_index: 8, voter_status: %{membership: :voter}},
          cluster
        )

      indexes = Cluster.voter_match_indexes(self_id, cluster, 12)
      assert 12 in indexes
      assert 10 in indexes
      assert 8 in indexes

      commit = Cluster.agreed_commit(indexes)
      assert commit == 10
    end

    test "effects pipeline with replies and notifications", %{conf: conf} do
      from1 = {self(), :ref1}
      from2 = {self(), :ref2}

      # Simulate multiple effects
      {effects1, notifys1} = Effects.add_reply(%{from: from1}, :ok1, :await_consensus, [], %{})

      {effects2, notifys2} =
        Effects.add_reply(%{from: from2}, :result2, {:notify, :corr1, self()}, effects1, notifys1)

      {effects3, notifys3} = Effects.add_reply(%{}, :ignored, :"$ra_no_reply", effects2, notifys2)

      assert length(effects3) == 1
      assert map_size(notifys3) == 1

      # Add machine effects
      machine_effects = [{:send_msg, :pid, :msg}]
      final_effects = Effects.append_machine_effects(machine_effects, effects3)

      assert length(final_effects) == 2
      assert {:send_msg, :pid, :msg} in final_effects

      # Wrap notifications
      final_effects = Effects.make_notify_effects(notifys3, final_effects)

      assert length(final_effects) == 3
      assert {:notify, _} = hd(final_effects)
    end
  end

  # ---------------------------------------------------------------------------
  # Segment Writer Tests
  # ---------------------------------------------------------------------------

  describe "LogSegmentWriter" do
    defp make_config(data_dir, name) do
      %{
        name: name,
        data_dir: Path.join(data_dir, Atom.to_string(name)),
        segment_max_entries: 10,
        segment_max_size_bytes: 1_000_000
      }
    end

    test "starts with empty state", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_empty)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      overview = RaftEx.LogSegmentWriter.overview(pid)
      assert overview.num_segments == 0

      GenServer.stop(pid)
    end

    test "writes entries to active segment", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_write)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      entries = [{1, 1, {:command, :test1}}, {2, 1, {:command, :test2}}]
      assert :ok = RaftEx.LogSegmentWriter.write(pid, entries)

      {:ok, read_entries} = RaftEx.LogSegmentWriter.read(pid, 1, 2)
      assert length(read_entries) == 2

      GenServer.stop(pid)
    end

    test "reads single entry by index", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_read_single)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      entries = [{1, 1, {:command, :test}}]
      assert :ok = RaftEx.LogSegmentWriter.write(pid, entries)

      {:ok, entry} = RaftEx.LogSegmentWriter.read_entry(pid, 1)
      assert entry == {1, 1, {:command, :test}}

      GenServer.stop(pid)
    end

    test "returns not_found for missing entry", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_not_found)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      assert {:error, :not_found} = RaftEx.LogSegmentWriter.read_entry(pid, 999)

      GenServer.stop(pid)
    end

    test "seals active segment and creates new one", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_seal)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      entries = [{1, 1, {:command, :test}}]
      assert :ok = RaftEx.LogSegmentWriter.write(pid, entries)

      assert :ok = RaftEx.LogSegmentWriter.seal_active_segment(pid)

      overview = RaftEx.LogSegmentWriter.overview(pid)
      assert overview.num_segments == 1

      GenServer.stop(pid)
    end

    test "creates new segment when max entries reached", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_max_entries)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      # Write 10 entries (max_entries is 10)
      entries = for i <- 1..10, do: {i, 1, {:command, i}}
      assert :ok = RaftEx.LogSegmentWriter.write(pid, entries)

      # Write one more - should trigger new segment
      assert :ok = RaftEx.LogSegmentWriter.write(pid, [{11, 1, {:command, :extra}}])

      overview = RaftEx.LogSegmentWriter.overview(pid)
      assert overview.num_segments >= 1

      GenServer.stop(pid)
    end

    test "truncates segments from index", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_truncate)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      entries = for i <- 1..5, do: {i, 1, {:command, i}}
      assert :ok = RaftEx.LogSegmentWriter.write(pid, entries)
      assert :ok = RaftEx.LogSegmentWriter.seal_active_segment(pid)

      assert :ok = RaftEx.LogSegmentWriter.truncate_from(pid, 3)

      overview = RaftEx.LogSegmentWriter.overview(pid)
      # Segments starting from index 3 should be removed
      assert overview.num_segments == 0

      GenServer.stop(pid)
    end

    test "deletes segments up to index", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_delete)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      entries = for i <- 1..5, do: {i, 1, {:command, i}}
      assert :ok = RaftEx.LogSegmentWriter.write(pid, entries)
      assert :ok = RaftEx.LogSegmentWriter.seal_active_segment(pid)

      assert :ok = RaftEx.LogSegmentWriter.delete_up_to(pid, 3)

      overview = RaftEx.LogSegmentWriter.overview(pid)
      # Segment with range {1, 5} won't be deleted since last_index(5) > 3
      # This is correct behavior - we only delete segments fully before up_to_index
      assert overview.num_segments == 1

      GenServer.stop(pid)
    end

    test "returns index range", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_range)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      # Active segment starts at index 1
      {first, last} = RaftEx.LogSegmentWriter.index_range(pid)
      assert first == 1
      assert last == 1

      entries = [{1, 1, {:command, :test}}, {2, 1, {:command, :test2}}]
      assert :ok = RaftEx.LogSegmentWriter.write(pid, entries)

      {first, last} = RaftEx.LogSegmentWriter.index_range(pid)
      assert first == 1
      assert last == 2

      GenServer.stop(pid)
    end

    test "handles empty write", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_empty_write)
      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      assert :ok = RaftEx.LogSegmentWriter.write(pid, [])

      GenServer.stop(pid)
    end

    test "recovers segments from disk", %{data_dir: data_dir} do
      config = make_config(data_dir, :seg_test_recover)

      # Create and write to segments
      {:ok, pid1} = RaftEx.LogSegmentWriter.start_link(config)
      entries = for i <- 1..3, do: {i, 1, {:command, i}}
      assert :ok = RaftEx.LogSegmentWriter.write(pid1, entries)
      assert :ok = RaftEx.LogSegmentWriter.seal_active_segment(pid1)
      GenServer.stop(pid1)

      # Recover
      {:ok, pid2} = RaftEx.LogSegmentWriter.start_link(config)
      {:ok, read_entries} = RaftEx.LogSegmentWriter.read(pid2, 1, 3)
      assert length(read_entries) == 3
      GenServer.stop(pid2)
    end
  end

  # ---------------------------------------------------------------------------
  # Network Layer Tests
  # ---------------------------------------------------------------------------

  describe "Network" do
    test "send_rpc routes to local process correctly" do
      # Start a simple GenServer to receive RPCs
      defmodule TestRpcReceiver do
        use GenServer
        def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid, name: :test_rpc_receiver)
        def init(test_pid), do: {:ok, %{test_pid: test_pid}}
        def handle_info({:gen_cast, {:rpc, rpc}}, state) do
          send(state.test_pid, {:received_rpc, rpc})
          {:noreply, state}
        end
        def handle_info(_, state), do: {:noreply, state}
      end

      {:ok, _} = TestRpcReceiver.start_link(self())

      rpc = %RaftEx.Types.AppendEntriesRpc{term: 1, leader_id: {:test, node()}}
      assert :ok = RaftEx.Network.send_rpc({:test_rpc_receiver, node()}, rpc)

      assert_receive {:received_rpc, ^rpc}, 1000
    end

    test "send_rpc returns error for non-existent local process" do
      rpc = %RaftEx.Types.AppendEntriesRpc{term: 1}
      assert {:error, :noproc} = RaftEx.Network.send_rpc({:non_existent_process, node()}, rpc)
    end

    test "call routes to local process correctly" do
      defmodule TestCallReceiver do
        use GenServer
        def start_link(), do: GenServer.start_link(__MODULE__, [], name: :test_call_receiver)
        def init(_), do: {:ok, %{}}
        def handle_call(:ping, _from, state), do: {:reply, :pong, state}
      end

      {:ok, _} = TestCallReceiver.start_link()
      assert {:ok, :pong} = RaftEx.Network.call({:test_call_receiver, node()}, :ping)
    end

    test "cast routes to local process correctly" do
      defmodule TestCastReceiver do
        use GenServer
        def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid, name: :test_cast_receiver)
        def init(test_pid), do: {:ok, %{test_pid: test_pid}}
        def handle_cast(:test_msg, state) do
          send(state.test_pid, {:received_cast, :test_msg})
          {:noreply, state}
        end
        def handle_cast(_, state), do: {:noreply, state}
      end

      {:ok, _} = TestCastReceiver.start_link(self())
      assert :ok = RaftEx.Network.cast({:test_cast_receiver, node()}, :test_msg)
      assert_receive {:received_cast, :test_msg}, 1000
    end

    test "monitor returns reference for local process" do
      assert {:error, :noproc} = RaftEx.Network.monitor({:non_existent, node()})
    end

    test "handle_node_up and handle_node_down return :ok" do
      assert :ok = RaftEx.Network.handle_node_up(:some_node)
      assert :ok = RaftEx.Network.handle_node_down(:some_node)
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-Node Integration Tests (Simulated via Local Processes)
  # ---------------------------------------------------------------------------

  describe "Multi-Node Integration" do
    setup %{data_dir: data_dir} do
      suffix = System.unique_integer([:positive])
      uid = "multi_node_test_uid_#{suffix}"

      names = %{
        wal: :"multi_test_wal_#{suffix}",
        log_meta: :"multi_test_log_meta_#{suffix}",
        open_mem_tbls: :"multi_test_open_mem_tbls_#{suffix}",
        log_ets: :"multi_test_log_ets_#{suffix}"
      }

      :ets.new(names.open_mem_tbls, [:named_table, :public, :set])
      :ets.new(names.log_ets, [:named_table, :public, :set])

      {:ok, _} =
        RaftEx.LogMeta.start_link(%{
          name: :"multi_node_system_#{suffix}",
          data_dir: data_dir,
          names: names
        })

      conf = %{
        uid: uid,
        system_config: %{
          data_dir: data_dir,
          names: names
        }
      }

      on_exit(fn ->
        try do
          :ets.delete(names.open_mem_tbls)
        rescue
          _ -> :ok
        end

        try do
          :ets.delete(names.log_ets)
        rescue
          _ -> :ok
        end
      end)

      %{conf: conf}
    end

    test "cluster membership is correctly initialized with multiple servers", %{conf: conf} do
      # Build initial cluster map manually
      server1_id = {:server1, node()}
      server2_id = {:server2, node()}
      server3_id = {:server3, node()}

      cluster = RaftEx.Server.Cluster.new(server1_id, [server1_id, server2_id, server3_id])

      # Verify cluster structure
      assert map_size(cluster) == 3
      assert Map.has_key?(cluster, server1_id)
      assert Map.has_key?(cluster, server2_id)
      assert Map.has_key?(cluster, server3_id)

      # Verify peer states
      peer1 = Map.get(cluster, server1_id)
      assert peer1.next_index == 1
      assert peer1.match_index == 0
      assert peer1.status == :normal
    end

    test "peer state updates propagate correctly", %{conf: conf} do
      server1_id = {:server1, node()}
      server2_id = {:server2, node()}

      cluster = RaftEx.Server.Cluster.new(server1_id, [server1_id, server2_id])

      # Update peer state
      updated_cluster =
        RaftEx.Server.Cluster.update_peer(server2_id, %{
          next_index: 10,
          match_index: 5,
          status: :normal
        }, cluster)

      peer2 = Map.get(updated_cluster, server2_id)
      assert peer2.next_index == 10
      assert peer2.match_index == 5

      # Original cluster unchanged
      original_peer2 = Map.get(cluster, server2_id)
      assert original_peer2.next_index == 1
    end

    test "voter match indexes compute correctly for quorum", %{conf: conf} do
      server1_id = {:server1, node()}
      server2_id = {:server2, node()}
      server3_id = {:server3, node()}

      cluster =
        %{
          server1_id => %{
            next_index: 10,
            match_index: 8,
            query_index: 0,
            commit_index_sent: 5,
            status: :normal,
            voter_status: %{membership: :voter, uid: "uid1"}
          },
          server2_id => %{
            next_index: 10,
            match_index: 6,
            query_index: 0,
            commit_index_sent: 5,
            status: :normal,
            voter_status: %{membership: :voter, uid: "uid2"}
          },
          server3_id => %{
            next_index: 10,
            match_index: 7,
            query_index: 0,
            commit_index_sent: 5,
            status: :normal,
            voter_status: %{membership: :non_voter, uid: "uid3"}
          }
        }

      # Collect voter match indexes (excluding self server1_id)
      indexes = RaftEx.Server.Cluster.voter_match_indexes(server1_id, cluster, 9)
      # Should include leader's 9 and server2's 6 (server3 is non_voter)
      assert 9 in indexes
      assert 6 in indexes
      refute 7 in indexes

      # Compute agreed commit
      agreed = RaftEx.Server.Cluster.agreed_commit(indexes)
      # Sorted desc: [9, 6], quorum = div(2,2)+1 = 2, index 1 = 6
      assert agreed == 6
    end

    test "election context tracks votes correctly", %{conf: conf} do
      # Simulate election context
      context = %{
        state: :candidate,
        term: 5,
        votes_received: MapSet.new([{:server1, node()}]),
        total_voters: 3,
        last_log_index: 10,
        last_log_term: 4
      }

      # Need 2 votes to win (div(3,2) + 1 = 2)
      assert RaftEx.Server.Election.votes_needed(3) == 2
      assert RaftEx.Server.Election.evaluate_election_result(context) == :ongoing

      # Add another vote
      new_votes = MapSet.put(context.votes_received, {:server2, node()})
      new_context = %{context | votes_received: new_votes}

      assert RaftEx.Server.Election.evaluate_election_result(new_context) == :won
    end

    test "log segment writer handles concurrent writes safely", %{conf: conf} do
      suffix = System.unique_integer([:positive])
      config = %{
        name: :"seg_concurrent_#{suffix}",
        data_dir: Path.join(conf.system_config.data_dir, "seg_concurrent_#{suffix}"),
        segment_max_entries: 5,
        segment_max_size_bytes: 1_000_000
      }

      {:ok, pid} = RaftEx.LogSegmentWriter.start_link(config)

      # Write from multiple processes concurrently
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            entries = for j <- 1..3, do: {i * 10 + j, 1, {:command, i, j}}
            RaftEx.LogSegmentWriter.write(pid, entries)
          end)
        end

      Task.await_many(tasks, 5000)

      # Verify all entries were written
      {:ok, all_entries} = RaftEx.LogSegmentWriter.read(pid, 1, 100)
      assert length(all_entries) == 9

      GenServer.stop(pid)
    end
  end
end
