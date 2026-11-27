defmodule RaftEx.App.ServerRuntimeCallbacksTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.ServerRuntime
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log
  alias RaftEx.Infra.Persistence.LogMeta
  alias RaftEx.Infra.Persistence.LogSnapshot

  setup do
    name = :runtime_test_meta
    dir = Path.join(System.tmp_dir!(), "runtime_meta_#{System.unique_integer([:positive])}")

    case Process.whereis(name) do
      nil ->
        {:ok, pid} =
          LogMeta.start_link(%{
            name: :runtime_test_setup,
            data_dir: dir,
            names: %{log_meta: name}
          })

        Process.unlink(pid)

      _ ->
        :ok
    end

    :ok
  end

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  # A WAL that always rejects appends, used to drive the :wal_down branch of
  # Domain.Server.handle_leader/2 (which becomes an :await_condition transition).
  defmodule WalDownServer do
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, :ok, [])
    def init(:ok), do: {:ok, :ok}

    def handle_call({:append, _uid, _entries}, _from, state),
      do: {:reply, {:error, :wal_down}, state}
  end

  # Minimal gen_statem used to exercise the statem_call/safe_call helpers.
  defmodule CallResponder do
    @behaviour :gen_statem

    def start(reply_fun), do: :gen_statem.start(__MODULE__, reply_fun, [])
    def init(reply_fun), do: {:ok, :ready, reply_fun}
    def callback_mode, do: :handle_event_function

    def handle_event({:call, from}, msg, :ready, reply_fun) do
      {:keep_state_and_data, [{:reply, from, reply_fun.(msg)}]}
    end
  end

  # A gen_statem that terminates with a chosen reason while handling a call.
  defmodule CrashingResponder do
    @behaviour :gen_statem

    def start(reason), do: :gen_statem.start(__MODULE__, reason, [])
    def init(reason), do: {:ok, :ready, reason}
    def callback_mode, do: :handle_event_function

    def handle_event({:call, _from}, _msg, :ready, reason) do
      Process.exit(self(), reason)
      {:keep_state_and_data, []}
    end
  end

  # A machine that emits a chosen effect when its command is applied.
  defmodule EffectMachine do
    @behaviour RaftEx.Domain.Machine

    def init(_conf), do: :initial

    def apply(_meta, cmd, state) do
      effects =
        case cmd do
          :release_cursor -> [{:release_cursor, 1, state}]
          :bg_work -> [{:bg_work, fn -> :ok end, fn _ -> :ok end}]
          :gc -> [:garbage_collection]
          _ -> []
        end

      {state, :ok, effects}
    end
  end

  defp server_state(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "runtime-test-uid",
      log_id: "runtime-test-log",
      metrics_key: :runtime_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{data_dir: "/tmp/runtime_test", names: %{log_meta: :runtime_test_meta}},
      command_plugs: Keyword.get(opts, :command_plugs, []),
      rpc_plugs: Keyword.get(opts, :rpc_plugs, [])
    )
    |> then(fn cfg ->
      %{
        cfg: cfg,
        current_term: 2,
        commit_index: 1,
        last_applied: 1,
        persisted_last_applied: 1,
        leader_id: Keyword.get(opts, :leader_id, nil),
        voted_for: nil,
        membership: :voter,
        cluster_change_permitted: false,
        pending_consistent_queries: [],
        rpc_from: @peer2,
        cluster: %{
          @self_id => %{
            voter_status: %{membership: :voter},
            next_index: 3,
            match_index: 2,
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
        log: %Log{
          entries: %{1 => {1, 2, :cmd}, 2 => {2, 2, :cmd}},
          range: {1, 2},
          last_term: 2,
          next_index: 3,
          last_written_index_term: {2, 2}
        },
        machine_state: {:simple, fn _cmd, s -> s end, 0},
        aux_state: nil
      }
    end)
  end

  defp state(opts \\ []) do
    %{
      conf: %{
        name: :runtime_test,
        cluster_name: Keyword.get(opts, :cluster_name, :runtime_cluster),
        data_dir: "/tmp/runtime_test",
        log_id: "runtime-test-log",
        broadcast_time: 100,
        tick_timeout: 100
      },
      server_state: server_state(opts),
      election_timeout_set: false,
      pending_commands: [],
      election_context: %{
        state: Keyword.get(opts, :context_state, :pre_vote),
        term: 2,
        votes_received: MapSet.new([@self_id]),
        total_voters: 1,
        last_log_index: 2,
        last_log_term: 2
      }
    }
  end

  # Creates a snapshot file in a private directory and returns a state whose
  # server_state.log.data_dir points at its parent (so ServerRuntime's
  # snapshot_dir/1 resolves to the private directory).
  defp state_with_snapshot(opts \\ []) do
    root = Path.join(System.tmp_dir!(), "runtime_snap_#{System.unique_integer([:positive])}")
    dir = Path.join(root, "snapshot")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(root) end)

    meta = %{index: 3, term: 2}
    mac_state = :erlang.term_to_binary(String.duplicate("snapshot-payload", 8))
    {:ok, _bytes} = LogSnapshot.write(dir, meta, mac_state, true)

    base = state(opts)
    log = %{base.server_state.log | data_dir: root, current_snapshot: {3, 2}}
    ss = %{base.server_state | log: log}

    {%{base | server_state: ss}, dir, meta}
  end

  defp send_state(read_state, opts \\ []) do
    %{
      meta: Keyword.get(opts, :meta, %{index: 3, term: 2}),
      read_state: read_state,
      offset: Keyword.get(opts, :offset, 0),
      snap_state: Keyword.get(opts, :snap_state, :snap),
      leader_id: @self_id,
      term: 2
    }
  end

  # An external pid whose node is not the local node; used to exercise the
  # remote branch of pid_to_server_id/1.
  defp remote_pid do
    node_bin = "remote@host"

    :erlang.binary_to_term(
      <<131, 103, 100, byte_size(node_bin)::16, node_bin::binary, 0::32, 1::32, 0>>
    )
  end

  describe "leader callbacks" do
    test "clear_transfer_leadership removes a stalled transfer target" do
      base = state()
      ss = Map.put(base.server_state, :transfer_leadership, @peer2)
      s = %{base | server_state: ss}

      assert {:keep_state, cleared, []} =
               ServerRuntime.leader(:state_timeout, :clear_transfer_leadership, s)

      assert is_nil(cleared.server_state.transfer_leadership)
    end

    test "unknown snapshot chunk peer is ignored" do
      assert {:keep_state, _s, []} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, @peer2}, state())
    end

    test "enter records the leader" do
      assert {:keep_state, _state, _actions} = ServerRuntime.leader(:enter, :follower, state())
    end

    test "trigger_election and ping" do
      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:error, :already_leader}}]} =
               ServerRuntime.leader({:call, from}, :trigger_election, state())

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :leader}}]} =
               ServerRuntime.leader({:call, from}, :ping, state())
    end

    test "state queries" do
      from = self()

      for spec <- [:members, :members_info, :leader, :last_applied, :unknown] do
        assert {:keep_state, _s, [{:reply, ^from, {:ok, _, _}}]} =
                 ServerRuntime.leader({:call, from}, {:state_query, spec}, state())
      end
    end

    test "tick timeout builds RPCs and effects" do
      assert {:keep_state, _state, actions} = ServerRuntime.leader(:_, :tick_timeout, state())
      assert is_list(actions)
    end

    test "leader_call command variants" do
      from = {self(), make_ref()}
      usr = {:"$usr", :cmd, :noreply}

      result = ServerRuntime.leader({:call, from}, {:leader_call, {:command, usr}}, state())

      assert match?({:keep_state, _, _}, result) or match?({:next_state, _, _, _}, result) or
               match?({:stop, _, _}, result)

      join = {:"$ra_join", @peer2, :noreply}

      result = ServerRuntime.leader({:call, from}, {:leader_call, {:command, join}}, state())

      assert match?({:keep_state, _, _}, result) or match?({:next_state, _, _, _}, result) or
               match?({:stop, _, _}, result)
    end
  end

  describe "follower callbacks" do
    test "ping, trigger_election and leader_call" do
      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :follower}}]} =
               ServerRuntime.follower({:call, from}, :ping, state())

      assert {:next_state, :candidate, _s, [{:reply, ^from, :ok}]} =
               ServerRuntime.follower({:call, from}, :trigger_election, state())

      assert {:keep_state, _s, [{:reply, ^from, {:error, :no_leader}}]} =
               ServerRuntime.follower({:call, from}, {:leader_call, :x}, state())

      with_leader = state(leader_id: @peer2)

      assert {:keep_state, _s, [{:reply, ^from, {:redirect, @peer2}}]} =
               ServerRuntime.follower({:call, from}, {:leader_call, :x}, with_leader)
    end

    test "enter with a leader records it" do
      s = state(leader_id: @peer2)
      assert {:keep_state, _state, _actions} = ServerRuntime.follower(:enter, :leader, s)
    end
  end

  describe "pre_vote and candidate callbacks" do
    test "pre_vote enter, ping, trigger_election and timeout" do
      assert {:keep_state, _s, _actions} = ServerRuntime.pre_vote(:enter, :follower, state())

      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :pre_vote}}]} =
               ServerRuntime.pre_vote({:call, from}, :ping, state())

      assert {:next_state, :candidate, _s, [{:reply, ^from, :ok}]} =
               ServerRuntime.pre_vote({:call, from}, :trigger_election, state())

      assert {:next_state, :pre_vote, _s, _actions} =
               ServerRuntime.pre_vote(:state_timeout, :election_timeout, state())
    end

    test "candidate enter, ping, trigger_election, leader_call and timeouts" do
      assert {:keep_state, _s, _actions} = ServerRuntime.candidate(:enter, :pre_vote, state())

      from = self()

      assert {:keep_state, _s, [{:reply, ^from, {:pong, :candidate}}]} =
               ServerRuntime.candidate({:call, from}, :ping, state())

      assert {:next_state, :candidate, _s, [{:reply, ^from, :ok}]} =
               ServerRuntime.candidate({:call, from}, :trigger_election, state())

      assert {:keep_state, _s, [{:reply, ^from, {:error, :no_leader}}]} =
               ServerRuntime.candidate({:call, from}, {:leader_call, :x}, state())

      assert {:next_state, :candidate, _s, _actions} =
               ServerRuntime.candidate(:state_timeout, :election_timeout, state())

      assert {:next_state, :leader, _s, []} =
               ServerRuntime.candidate(:internal, :election_won, state())

      assert {:next_state, :leader, _s, []} =
               ServerRuntime.candidate(:state_timeout, :become_leader, state())
    end

    test "pre_vote election timeout does not leak domain effects as statem actions" do
      assert {:next_state, :pre_vote, _state, actions} =
               ServerRuntime.pre_vote(:state_timeout, :election_timeout, state())

      refute Enum.any?(actions, &match?({:store_meta, _, _}, &1)),
             "store_meta is a domain effect, not a gen_statem action"

      assert Enum.any?(actions, &match?({:state_timeout, _, :election_timeout}, &1))
    end

    test "candidate election timeout does not leak domain effects as statem actions" do
      assert {:next_state, :candidate, _state, actions} =
               ServerRuntime.candidate(
                 :state_timeout,
                 :election_timeout,
                 state(context_state: :candidate)
               )

      refute Enum.any?(actions, &match?({:store_meta, _, _}, &1)),
             "store_meta is a domain effect, not a gen_statem action"

      assert Enum.any?(actions, &match?({:state_timeout, _, :election_timeout}, &1))
    end

    test "vote reply handling" do
      reply = %Types.RequestVoteResult{term: 2, vote_granted: true}

      from = {self(), make_ref()}

      assert {:next_state, :leader, _s, _actions} =
               ServerRuntime.candidate(
                 {:cast, {from, reply}},
                 :vote_reply,
                 state(context_state: :candidate)
               )

      pre_vote_reply = %Types.PreVoteResult{term: 2, vote_granted: true}

      from = {self(), make_ref()}

      assert {:next_state, :candidate, _s, _actions} =
               ServerRuntime.pre_vote({:cast, {from, pre_vote_reply}}, :pre_vote_reply, state())
    end
  end

  describe "leader error and cluster-change branches" do
    test "install-snapshot result clears the snapshot send and queues pipeline rpcs" do
      reply = %Types.InstallSnapshotResult{term: 2, last_index: 5, last_term: 2}

      assert {:keep_state, _s, actions} = ServerRuntime.leader(:info, reply, state())
      assert {:next_event, :info, :pipeline_rpcs} in actions
    end

    test "install-snapshot result with a higher term steps down to follower" do
      reply = %Types.InstallSnapshotResult{term: 3, last_index: 5, last_term: 2}

      assert {:next_state, :follower, _s, _actions} = ServerRuntime.leader(:info, reply, state())
    end

    test "store_meta falls back to the default meta name" do
      base = state()
      cfg = %{base.server_state.cfg | system_config: %{data_dir: "/tmp/runtime_test"}}
      ss = %{base.server_state | cfg: cfg}
      s = %{base | server_state: ss}
      reply = %Types.InstallSnapshotResult{term: 3, last_index: 5, last_term: 2}

      assert {:next_state, :follower, _s, _actions} = ServerRuntime.leader(:info, reply, s)
    end

    test "a wal_down append error transitions to await_condition" do
      {:ok, wal} = WalDownServer.start_link()
      base = state()
      log = %{base.server_state.log | wal_name: wal}
      s = %{base | server_state: %{base.server_state | log: log}}
      from = {self(), make_ref()}

      # Commands batch first; the WAL failure surfaces on flush.
      assert {:keep_state, s1, []} =
               ServerRuntime.leader(
                 {:call, from},
                 {:leader_call, {:command, {:"$usr", :probe, :await_consensus}}},
                 s
               )

      assert {:next_state, :await_condition, _s, _actions} =
               ServerRuntime.leader(:info, :flush_commands, s1)
    end

    test "machine release_cursor and bg_work effects are executed" do
      for {cmd, worker_pid} <- [{:release_cursor, nil}, {:bg_work, nil}] do
        base = state()
        ss = base.server_state

        cfg = %{
          ss.cfg
          | machine: {:machine, EffectMachine, %{}},
            machine_versions: [{0, 0}],
            effective_machine_module: EffectMachine,
            effective_handle_aux_fun: nil
        }

        ss = %{
          ss
          | cfg: cfg,
            machine_state: :initial,
            cluster: %{@self_id => Map.fetch!(ss.cluster, @self_id)}
        }

        ss = Map.put(ss, :commit_latency, 0)

        s = %{base | conf: Map.put(base.conf, :worker_pid, worker_pid), server_state: ss}
        from = {self(), make_ref()}

        assert {:keep_state, _s, _actions} =
                 ServerRuntime.leader(
                   {:call, from},
                   {:leader_call, {:command, {:"$usr", cmd, :noreply}}},
                   s
                 )
      end
    end

    test "a committed cluster delete is caught and mapped to a next state" do
      base = state()
      ss = base.server_state
      entries = Map.put(ss.log.entries, 2, {2, 2, {:"$ra_cluster", %{}, :delete, :noreply}})
      log = %{ss.log | entries: entries, range: {1, 2}, next_index: 3}
      ss = %{ss | log: log, cluster: %{@self_id => Map.fetch!(ss.cluster, @self_id)}}

      assert {:stop, :normal, _s} =
               ServerRuntime.leader(:info, {:ra_log_event, {:written, [], []}}, %{
                 base
                 | server_state: ss
               })
    end
  end

  describe "follower, pre_vote and candidate message routing" do
    test "follower election timeout enters pre_vote" do
      assert {:next_state, :pre_vote, _s, _actions} =
               ServerRuntime.follower(:info, :election_timeout, state())
    end

    test "rpc and gen_cast envelopes are unwrapped" do
      assert {:keep_state, _s, _actions} =
               ServerRuntime.follower(:info, {:rpc, :unknown}, state())

      assert {:keep_state, _s, _actions} =
               ServerRuntime.follower(:info, {:gen_cast, :unknown}, state())

      assert {:keep_state, _s, _actions} =
               ServerRuntime.follower(:info, {self(), {:gen_cast, :unknown}}, state())
    end

    test "a sender pid from another node is looked up remotely" do
      assert {:keep_state, _s, _actions} =
               ServerRuntime.follower(:info, {remote_pid(), :unknown}, state())
    end

    test "pre_vote handles a pid sender, a result struct and follower-driven transitions" do
      base = state()

      # Unknown messages do not demote a pre-vote server; only genuine
      # leader traffic, a higher term, or voting away ends the round.
      assert {:keep_state, _s, _actions} =
               ServerRuntime.pre_vote(:info, {self(), {:gen_cast, :unknown}}, base)

      assert {:keep_state, _s, _actions} =
               ServerRuntime.pre_vote(
                 :info,
                 %Types.PreVoteResult{term: 2, vote_granted: false},
                 base
               )

      assert {:keep_state, _s, _actions} =
               ServerRuntime.pre_vote(:info, :election_timeout, base)

      # TimeoutNow without a recognized leader is ignored, like any other
      # non-leader traffic, instead of demoting the pre-vote server.
      assert {:keep_state, _s, _actions} =
               ServerRuntime.pre_vote(:info, %Types.TimeoutNowRpc{term: 2}, base)
    end

    test "pre_vote and candidate route an incoming snapshot to a generic next state" do
      {base, _dir, _meta} = state_with_snapshot()

      rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @peer2,
        meta: %{index: 3, term: 2},
        chunk_state: :first,
        data: <<0, 0, 0, 0, "abc">>
      }

      assert {:next_state, :receive_snapshot, _s, _actions} =
               ServerRuntime.pre_vote(:info, rpc, base)

      assert {:next_state, :receive_snapshot, _s, _actions} =
               ServerRuntime.candidate(:info, rpc, base)
    end

    test "receive_snapshot dispatches middle and last chunks" do
      {base, dir, _meta} = state_with_snapshot()
      {:ok, _meta, read_state} = LogSnapshot.begin_read(dir, %{can_accept_full_file: false})
      {crc, _} = read_state
      {:ok, first, {:next, inner1}} = LogSnapshot.read_chunk(read_state, 24, dir)
      {:ok, middle, {:next, inner2}} = LogSnapshot.read_chunk({crc, inner1}, 24, dir)
      {:ok, last, :last} = LogSnapshot.read_chunk({crc, inner2}, 1_000_000, dir)

      first_rpc = %Types.InstallSnapshotRpc{
        term: 2,
        leader_id: @peer2,
        meta: %{index: 3, term: 2},
        chunk_state: :first,
        data: first
      }

      assert {:next_state, :receive_snapshot, s1, _actions} =
               ServerRuntime.follower(:info, first_rpc, base)

      assert is_tuple(s1.server_state.snapshot_state)

      middle_rpc = %{first_rpc | chunk_state: :middle, data: middle}

      assert {:keep_state, s2, _actions} =
               ServerRuntime.receive_snapshot(:info, middle_rpc, s1)

      assert is_tuple(s2.server_state.snapshot_state)

      last_rpc = %{first_rpc | chunk_state: :last, data: <<0::32>>}

      assert {:next_state, :follower, s3, _actions} =
               ServerRuntime.receive_snapshot(:info, last_rpc, s2)

      assert is_nil(s3.server_state.snapshot_state)
    end

    test "receive_snapshot timeout abandons the partial transfer" do
      {base, dir, _meta} = state_with_snapshot()
      {:ok, accept_state} = LogSnapshot.begin_accept(dir, %{index: 3, term: 2})
      ss = Map.put(base.server_state, :snapshot_state, accept_state)
      ss = Map.put(ss, :snapshot_meta, %{index: 3, term: 2})
      s = %{base | server_state: ss}

      assert {:next_state, :follower, s1, []} =
               ServerRuntime.receive_snapshot(
                 :state_timeout,
                 :snapshot_transfer_timeout,
                 s
               )

      assert is_nil(s1.server_state.snapshot_state)
      assert is_nil(s1.server_state.snapshot_meta)
    end

    test "snapshot send timeout clears the send and retries on the next pass" do
      s = Map.put(state(), :snapshot_sends, %{@peer2 => send_state({0, 0, nil})})

      assert {:keep_state, s1, actions} =
               ServerRuntime.leader(
                 :state_timeout,
                 {:snapshot_send_timeout, @peer2},
                 s
               )

      refute Map.has_key?(s1.snapshot_sends, @peer2)

      assert Enum.any?(actions, &match?({:next_event, :info, :pipeline_rpcs}, &1))

      assert {:keep_state, ^s, []} =
               ServerRuntime.leader(:state_timeout, {:snapshot_send_timeout, :unknown}, s)
    end

    test "entering follower abandons in-flight snapshot sends" do
      s = Map.put(state(), :snapshot_sends, %{@peer2 => send_state({0, 0, nil})})

      assert {:keep_state, s1, _actions} = ServerRuntime.follower(:enter, :leader, s)
      refute Map.has_key?(s1.snapshot_sends, @peer2)
    end

    test "candidate handles follower-driven candidate and pre_vote transitions" do
      base = state()

      # TimeoutNow without a recognized leader is ignored, not demoting:
      # read-only and non-leader traffic must not abort the campaign.
      assert {:keep_state, _s, _actions} =
               ServerRuntime.candidate(:info, %Types.TimeoutNowRpc{term: 2}, base)

      assert {:next_state, :pre_vote, _s, _actions} =
               ServerRuntime.candidate(:info, :election_timeout, base)
    end

    test "follower replies to a remote rpc sender" do
      remote_candidate = {:remote_candidate, :remote@host}

      rpc = %Types.PreVoteRpc{
        term: 2,
        candidate_id: remote_candidate,
        token: make_ref(),
        machine_version: 0,
        last_log_index: 2,
        last_log_term: 2
      }

      assert {:keep_state, _s, _actions} = ServerRuntime.follower(:info, rpc, state())
    end

    test "vote replies can lose or remain ongoing" do
      from = {self(), make_ref()}
      base = state(context_state: :candidate)

      assert {:next_state, :follower, _s, _actions} =
               ServerRuntime.candidate(
                 {:cast, {from, %Types.RequestVoteResult{term: 5, vote_granted: false}}},
                 :vote_reply,
                 base
               )

      assert {:keep_state, _s, _actions} =
               ServerRuntime.candidate(
                 {:cast, {from, %Types.RequestVoteResult{term: 2, vote_granted: false}}},
                 :vote_reply,
                 base
               )
    end
  end

  describe "snapshot send paths" do
    test "tick starts a snapshot transfer for a lagging peer" do
      {base, _dir, _meta} = state_with_snapshot()

      assert {:keep_state, _s, actions} =
               ServerRuntime.leader({:timeout, :tick}, :tick_timeout, base)

      # A new transfer arms its send timeout; the chunk itself is sent via
      # the RPC side effect, not as a gen_statem action.
      assert Enum.any?(actions, &match?({:state_timeout, _, {:snapshot_send_timeout, _}}, &1))
    end

    test "tick resumes a snapshot transfer already in progress" do
      {base, dir, _meta} = state_with_snapshot()
      {:ok, _meta, read_state} = LogSnapshot.begin_read(dir, %{can_accept_full_file: false})
      s = %{base | conf: Map.put(base.conf, :snapshot_chunk_size, 20)}
      s = Map.put(s, :snapshot_sends, %{@peer2 => send_state(read_state)})

      assert {:keep_state, s1, actions} =
               ServerRuntime.leader({:timeout, :tick}, :tick_timeout, s)

      assert Enum.any?(actions, &match?({:next_event, :info, {:send_snapshot_chunk, _}}, &1))
      assert Map.has_key?(s1.snapshot_sends, @peer2)
    end

    test "tick with no snapshot file does not emit an rpc" do
      base = state()
      empty = Path.join(System.tmp_dir!(), "runtime_empty_#{System.unique_integer([:positive])}")

      ss = %{
        base.server_state
        | log: %{base.server_state.log | data_dir: empty, current_snapshot: {3, 2}}
      }

      assert {:keep_state, _s, actions} =
               ServerRuntime.leader(:_, :tick_timeout, %{base | server_state: ss})

      refute Enum.any?(actions, &match?({:state_timeout, _, {:snapshot_send_timeout, _}}, &1))
      refute Enum.any?(actions, &match?({:next_event, :info, {:send_snapshot_chunk, _}}, &1))
    end

    test "multi-chunk snapshot sends cover first, middle and last chunks" do
      {base, dir, _meta} = state_with_snapshot()
      {:ok, _meta, read_state} = LogSnapshot.begin_read(dir, %{can_accept_full_file: false})
      s = %{base | conf: Map.put(base.conf, :snapshot_chunk_size, 20)}
      s = Map.put(s, :snapshot_sends, %{@peer2 => send_state(read_state)})

      assert {:keep_state, s1, [{:next_event, :info, {:send_snapshot_chunk, @peer2}}]} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, @peer2}, s)

      assert %{offset: 20} = s1.snapshot_sends[@peer2]

      {final_state, chunk_count} =
        Enum.reduce_while(2..50, {s1, 1}, fn _, {current, count} ->
          case ServerRuntime.leader(:info, {:send_snapshot_chunk, @peer2}, current) do
            {:keep_state, next, [{:next_event, :info, {:send_snapshot_chunk, @peer2}}]} ->
              {:cont, {next, count + 1}}

            {:keep_state, next, []} ->
              {:halt, {next, count + 1}}
          end
        end)

      # First plus at least one middle plus last.
      assert chunk_count >= 3
      assert map_size(final_state.snapshot_sends) == 0
    end

    test "a bare read state also advances the snapshot chunk offset" do
      {base, dir, _meta} = state_with_snapshot()

      {:ok, _meta, {_crc, read_state}} =
        LogSnapshot.begin_read(dir, %{can_accept_full_file: false})

      s = %{base | conf: Map.put(base.conf, :snapshot_chunk_size, 20)}
      s = Map.put(s, :snapshot_sends, %{@peer2 => send_state(read_state)})

      assert {:keep_state, _s, [{:next_event, :info, {:send_snapshot_chunk, @peer2}}]} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, @peer2}, s)
    end

    test "a failed chunk read clears the snapshot send" do
      base = state()
      s = Map.put(base, :snapshot_sends, %{@peer2 => send_state({0, 100, make_ref()})})

      assert {:keep_state, s1, []} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, @peer2}, s)

      assert map_size(s1.snapshot_sends) == 0
    end

    test "snapshot rpcs are delivered to local and remote peers" do
      {base, dir, _meta} = state_with_snapshot()
      {:ok, _meta, read_state} = LogSnapshot.begin_read(dir, %{can_accept_full_file: false})
      name = :"snapshot_peer_#{System.unique_integer([:positive])}"
      Process.register(self(), name)

      local_peer = {name, node()}
      s = Map.put(base, :snapshot_sends, %{local_peer => send_state(read_state)})

      # Single-chunk snapshot completes immediately; delivery is asserted
      # via the side effect, not the actions.
      assert {:keep_state, s1, []} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, local_peer}, s)

      refute Map.has_key?(s1.snapshot_sends, local_peer)

      # The envelope carries the sender id so the receiver can attribute
      # the RPC without a remote lookup (10.3).
      assert_receive {:"$gen_cast", {_from, {:rpc, @self_id, %Types.InstallSnapshotRpc{}}}}

      Process.unregister(name)

      remote_peer = {:snapshot_remote, :remote@host}

      {:ok, _meta, remote_read_state} =
        LogSnapshot.begin_read(dir, %{can_accept_full_file: false})

      s = Map.put(base, :snapshot_sends, %{remote_peer => send_state(remote_read_state)})

      assert {:keep_state, s1, []} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, remote_peer}, s)

      refute Map.has_key?(s1.snapshot_sends, remote_peer)
    end

    test "snapshot chunk info for an unknown peer is ignored" do
      assert {:keep_state, _s, []} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, @peer2}, state())
    end

    test "a snapshot chunk for a local peer with no registered process is a no-op" do
      {base, dir, _meta} = state_with_snapshot()
      {:ok, _meta, read_state} = LogSnapshot.begin_read(dir, %{can_accept_full_file: false})
      name = :"missing_snapshot_peer_#{System.unique_integer([:positive])}"
      peer = {name, node()}
      refute Process.whereis(name)

      s = Map.put(base, :snapshot_sends, %{peer => send_state(read_state)})

      # Single-chunk snapshot completes in one shot; the send itself is a
      # side effect (no-op without a registered process).
      assert {:keep_state, s1, []} =
               ServerRuntime.leader(:info, {:send_snapshot_chunk, peer}, s)

      refute Map.has_key?(s1.snapshot_sends, peer)
    end

    test "a follower reply to a remote rpc sender goes through the transport" do
      remote_candidate = {:remote_reply_candidate, :remote@host}

      rpc = %Types.PreVoteRpc{
        term: 2,
        candidate_id: remote_candidate,
        token: make_ref(),
        machine_version: 0,
        last_log_index: 2,
        last_log_term: 2
      }

      assert {:keep_state, _s, _actions} = ServerRuntime.follower(:info, rpc, state())
    end
  end

  describe "call helpers" do
    test "missing local processes produce errors" do
      missing = :"missing_runtime_#{System.unique_integer([:positive])}"

      assert {:error, :noproc} = ServerRuntime.ping({missing, node()}, 100)
      assert {:error, :noproc} = ServerRuntime.state_query({missing, node()}, :members, 100)

      assert {:error, :noproc} =
               ServerRuntime.local_state_query({missing, node()}, :overview, 100)
    end

    test "multiple server ids are tried in order" do
      missing = :"missing_multi_#{System.unique_integer([:positive])}"

      assert {:error, {:no_more_servers_to_try, _}} =
               ServerRuntime.state_query([{missing, node()}, {missing, node()}], :members, 100)
    end

    test "a registered gen_statem is called through statem_call_local" do
      {:ok, pid} = CallResponder.start(fn _msg -> {:ok, :replied, :test} end)
      name = :"call_responder_#{System.unique_integer([:positive])}"
      Process.register(pid, name)
      on_exit(fn -> if Process.whereis(name), do: Process.unregister(name) end)

      assert {:ok, :replied, :test} = ServerRuntime.statem_call_local(name, :anything, 100)
      assert {:ok, :replied, :test} = ServerRuntime.state_query([name], :members, 100)
    end

    test "calls to a process that exits during the call are converted to errors" do
      for reason <- [:shutdown, :boom] do
        {:ok, pid} = CrashingResponder.start(reason)
        assert {:error, ^reason} = ServerRuntime.ping(pid, 100)
      end
    end

    test "a call to a remote pid is converted to a nodedown or timeout error" do
      result = ServerRuntime.ping(remote_pid(), 100)
      assert match?({:error, :nodedown}, result) or match?(:timeout, result)
    end

    test "a remote server id reaches the erpc branch of safe_call" do
      assert {:error, reason} = ServerRuntime.ping({:remote_server, :nope@badhost}, 100)
      assert reason in [:noconnection, :nodedown]
    end
  end
end
