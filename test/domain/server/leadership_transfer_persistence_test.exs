defmodule RaftEx.Domain.Server.LeadershipTransferPersistenceTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.{Config, Election, RpcHandler}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.{Log, LogMeta}

  @leader {:leader, node()}
  @target {:target, node()}

  defp config(meta, id, uid) do
    struct!(Config,
      id: id,
      uid: uid,
      log_id: "transfer-log",
      metrics_key: :transfer_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{log_meta: meta}}
    )
  end

  defp state(meta, id, uid, term, leader_id) do
    %{
      cfg: config(meta, id, uid),
      current_term: term,
      leader_id: leader_id,
      voted_for: nil,
      cluster: %{},
      log: %Log{entries: %{}, range: nil, last_term: 0, next_index: 1}
    }
  end

  test "transfer persists the new term on target and old leader" do
    dir = Path.join(System.tmp_dir!(), "transfer_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    meta = :"transfer_meta_#{System.unique_integer([:positive])}"

    {:ok, pid} = LogMeta.start_link(%{name: :transfer, data_dir: dir, names: %{log_meta: meta}})
    Process.unlink(pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    target_uid = "transfer-target-uid"
    leader_uid = "transfer-leader-uid"

    # Target receives TimeoutNow from its recognized leader in the same term.
    target = state(meta, @target, target_uid, 3, @leader)
    rpc = %Types.TimeoutNowRpc{term: 3, leader_id: @leader}

    assert {:candidate, target_candidate, _} = Server.handle_follower(rpc, target)
    assert target_candidate.current_term == 3

    # Target campaigns in term 4; the candidate election persists term+vote.
    {ctx, effects} = Election.start_election(:candidate, target_candidate)
    assert ctx.term == 4

    for {:store_meta, key, value} <- effects do
      assert :ok = LogMeta.store_sync(meta, target_uid, key, value)
    end

    assert LogMeta.fetch(meta, target_uid, :current_term) == 4

    # Old leader observes the higher term and adopts it durably.
    leader = state(meta, @leader, leader_uid, 3, nil)
    leader_adopted = RpcHandler.adopt_term(leader, 4)

    assert leader_adopted.current_term == 4
    assert LogMeta.fetch(meta, leader_uid, :current_term) == 4
    assert LogMeta.fetch(meta, leader_uid, :voted_for) == nil

    # A restart from persisted state would report the transferred term.
    assert LogMeta.fetch(meta, target_uid, :current_term) == 4
  end
end
