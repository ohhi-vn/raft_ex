defmodule RaftEx.Domain.CommandBatchTest do
  @moduledoc "Unit coverage for Section 8 command batching (tasks 8.1-8.4)."
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Machine
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.LogMeta

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defmodule WalDown do
    use GenServer
    def start_link, do: GenServer.start_link(__MODULE__, :ok, [])
    def init(:ok), do: {:ok, :ok}

    def handle_call({:append, _uid, _entries}, _from, state),
      do: {:reply, {:error, :wal_down}, state}
  end

  setup do
    dir = RaftEx.TestSupport.tmp_dir("cmd_batch_test")
    meta_name = :"cmd_batch_meta_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogMeta, %{name: :cmd_batch, names: %{log_meta: meta_name}, data_dir: dir}}
    )

    Process.put(:cmd_batch_meta_name, meta_name)
    :ok
  end

  defp config(opts \\ []) do
    struct!(Config,
      id: @self_id,
      uid: "cmd-batch-uid",
      log_id: "cmd-batch-log",
      metrics_key: :cmd_batch_metrics,
      machine: {:machine, Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: Machine.Simple,
      command_plugs: Keyword.get(opts, :command_plugs, []),
      rpc_plugs: [],
      machine_versions: [{0, 0}],
      system_config: %{names: %{log_meta: Process.get(:cmd_batch_meta_name)}}
    )
  end

  defp log(opts \\ []) do
    %RaftEx.Infra.Persistence.Log{
      entries: %{},
      range: nil,
      last_term: 0,
      next_index: 1,
      last_written_index_term: {0, 0},
      wal_name: Keyword.get(opts, :wal_name, nil),
      meta_name: Process.get(:cmd_batch_meta_name),
      cfg: %{uid: "cmd-batch-uid"}
    }
  end

  defp state(opts \\ []) do
    %{
      cfg: config(opts),
      current_term: 2,
      commit_index: 0,
      last_applied: 0,
      persisted_last_applied: 0,
      leader_id: nil,
      voted_for: nil,
      rpc_from: nil,
      membership: :voter,
      cluster_change_permitted: true,
      cluster_index_term: {0, 0},
      commit_latency: 0,
      transfer_leadership: nil,
      became_leader_at: System.monotonic_time(:millisecond),
      pending_consistent_queries: [],
      query_index: 0,
      queries_waiting_heartbeats: :queue.new(),
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
      log: log(opts),
      machine_state: {:simple, fn _cmd, s -> {s, :ok} end, 0},
      aux_state: nil
    }
  end

  defp usr_cmd(i, from \\ nil) do
    {:"$usr", %{from: from}, {:cmd, i}, :await_consensus}
  end

  test "8.2 a homogeneous batch appends with consecutive indexes and one replication pass" do
    cmds = [usr_cmd(1), usr_cmd(2), usr_cmd(3)]
    assert {:leader, new_state, effects} = Server.handle_leader({:commands, cmds}, state())

    assert new_state.log.entries[1] == {1, 2, Enum.at(cmds, 0)}
    assert new_state.log.entries[2] == {2, 2, Enum.at(cmds, 1)}
    assert new_state.log.entries[3] == {3, 2, Enum.at(cmds, 2)}
    assert new_state.log.next_index == 4

    # One replication pass (single batch per peer), not one pass per command.
    rpcs = Enum.filter(effects, &match?({:send_rpc, @peer2, %Types.AppendEntriesRpc{}}, &1))
    assert length(rpcs) == 1
    [{:send_rpc, @peer2, %Types.AppendEntriesRpc{entries: entries}}] = rpcs
    assert length(entries) == 3
  end

  test "8.4 a WAL failure errors every caller in the batch and leaves the log untouched" do
    {:ok, wal} = WalDown.start_link()
    froms = for _ <- 1..3, do: {self(), make_ref()}
    cmds = Enum.with_index(froms, fn from, i -> usr_cmd(i, from) end)
    before_state = state(wal_name: wal)

    # A dead WAL parks the leader in await_condition (same as the single
    # path) with an error reply queued for every caller in the batch.
    assert {:await_condition, new_state, effects} =
             Server.handle_leader({:commands, cmds}, before_state)

    assert new_state.log.entries == %{}
    assert new_state.log.next_index == 1

    for from <- froms do
      assert Enum.any?(effects, fn
               {:reply, ^from, {:error, _}} -> true
               _ -> false
             end),
             "expected an error reply for #{inspect(from)} in #{inspect(effects)}"
    end
  end

  test "8.3 mixed batches preserve order by falling back to sequential handling" do
    cluster_cmd = {:"$ra_leave", self(), @peer2, :after_log_append}
    cmds = [usr_cmd(1), cluster_cmd, usr_cmd(2)]

    assert {:leader, new_state, _effects} = Server.handle_leader({:commands, cmds}, state())

    assert {1, 2, _} = new_state.log.entries[1]
    assert {2, 2, {:"$ra_cluster_change", _, _, _}} = new_state.log.entries[2]
    assert {3, 2, _} = new_state.log.entries[3]
  end

  test "8.1 an empty batch is a no-op" do
    s = state()
    assert {:leader, ^s, []} = Server.handle_leader({:commands, []}, s)
  end
end
