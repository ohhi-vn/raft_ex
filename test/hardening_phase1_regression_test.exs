defmodule RaftEx.HardeningPhase1RegressionTest do
  @moduledoc """
  Phase 1 regression harness for `raft-core-hardening-and-performance` task 1.2.

  Each test asserts the CORRECT behavior required by the change specs.
  All six currently FAIL against the unhardened implementation, documenting
  the confirmed defects. They must pass after Phase 1-5 fixes land.
  """
  use ExUnit.Case, async: false

  alias RaftEx.App.ServerRuntime
  alias RaftEx.Domain.Server.{Config, RpcHandler}
  alias RaftEx.Domain.Server.Effects
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.LogWal

  @self_id {:server1, node()}
  @leader {:leader, node()}
  @peer2 {:server2, node()}
  @peer3 {:server3, node()}

  defp config(data_dir, wal_name) do
    struct!(Config,
      id: @self_id,
      uid: "hardening-regression-uid",
      log_id: "log",
      metrics_key: :hardening_regression_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{data_dir: data_dir, names: %{wal: wal_name}}
    )
  end

  defp base_state(cfg) do
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
      cfg: cfg,
      current_term: 4,
      log: %{
        entries: %{},
        range: nil,
        last_term: 0,
        next_index: 1,
        wal_name: cfg.system_config.names.wal
      },
      cluster: %{@self_id => peer.(0), @peer2 => peer.(0), @peer3 => peer.(0)},
      commit_index: 0,
      leader_id: nil,
      voted_for: {@peer2, node()},
      snapshot_state: nil
    }
  end

  test "same-term heartbeat preserves voted_for" do
    data_dir = RaftEx.TestSupport.tmp_dir("hardening_hb")
    wal_name = :"hb_wal_#{System.unique_integer([:positive])}"
    start_supervised!({LogWal, %{names: %{wal: wal_name}, data_dir: data_dir}})

    state = base_state(config(data_dir, wal_name))
    rpc = %Types.HeartbeatRpc{query_index: 1, term: 4, leader_id: @leader, commit_index: 0}

    {new_state, _effects} = RpcHandler.handle_heartbeat_rpc(rpc, state)

    assert new_state.current_term == 4

    assert new_state.voted_for == {@peer2, node()},
           "same-term heartbeat must not clear voted_for (election safety)"
  end

  test "WAL prefix truncation removes bytes from disk" do
    base = RaftEx.TestSupport.tmp_dir("hardening_wal")
    wal_name = :"trunc_wal_#{System.unique_integer([:positive])}"
    start_supervised!({LogWal, %{names: %{wal: wal_name}, data_dir: base}})
    uid = "trunc-uid"

    entries = for i <- 1..10, do: {i, 1, {:cmd, i}}
    assert {:ok, 1, 10} = LogWal.append(wal_name, uid, entries)

    assert :ok = LogWal.truncate(wal_name, uid, 5)

    server_dir = Path.join(base, uid)
    {first, _last, recovered} = LogWal.recover(%{data_dir: server_dir})

    assert first == 6, "recovery after prefix truncation must start at 6, got #{first}"
    assert Enum.map(recovered, &elem(&1, 0)) == Enum.to_list(6..10)
  end

  test "receive_snapshot handles middle chunks instead of ignoring them" do
    data_dir = RaftEx.TestSupport.tmp_dir("hardening_snap")
    wal_name = :"snap_wal_#{System.unique_integer([:positive])}"
    start_supervised!({LogWal, %{names: %{wal: wal_name}, data_dir: data_dir}})

    ss =
      base_state(config(data_dir, wal_name))
      |> Map.merge(%{snapshot_state: {:partial, :in_progress}, rpc_from: @leader})

    state = %{server_state: ss, conf: %{}}

    event =
      {:rpc,
       %Types.InstallSnapshotRpc{
         term: 4,
         leader_id: @leader,
         meta: %{},
         chunk_state: :middle,
         data: <<0>>
       }}

    result = ServerRuntime.receive_snapshot(:info, event, state)

    refute match?({:keep_state, ^state, []}, result),
           "receive_snapshot must dispatch :middle chunks, not ignore every event"
  end

  test "await_condition re-evaluates instead of ignoring every event" do
    state = %{condition: %{predicate_fun: fn _ -> true end, transition_to: :leader}}
    result = ServerRuntime.await_condition(:info, :tick_timeout, state)

    refute match?({:keep_state, ^state, []}, result),
           "await_condition must re-evaluate its predicate, not ignore every event"
  end

  test "reply_from and pipeline notifications are routed by the runtime" do
    source = File.read!(Path.join([File.cwd!(), "lib", "app", "server_runtime.ex"]))

    assert source =~ "{:reply, from, {:wrap_reply, reply}, replier}",
           "ServerRuntime must handle 4-tuple {:reply, from, reply, replier} effects"

    assert source =~ "{:notify,",
           "ServerRuntime must handle {:notify, notifys} pipeline effects"
  end

  test "delete_cluster terminates instead of entering an undefined state" do
    source = File.read!(Path.join([File.cwd!(), "lib", "app", "server_runtime.ex"]))

    refute source =~ "def delete_and_terminate",
           "placeholder: runtime must not rely on an undefined :delete_and_terminate callback"

    assert function_exported?(ServerRuntime, :delete_and_terminate, 3) or
             (source =~ ":delete_and_terminate" and source =~ "{:stop, :normal"),
           "delete_cluster must terminate cleanly instead of crashing on an undefined state"
  end

  test "Effects produces routable reply and notify shapes" do
    me = self()

    {effects, _} =
      Effects.add_reply(%{from: me}, :ok, {:await_consensus, %{reply_from: :local}}, [], %{})

    assert [{:reply, _from, {:wrap_reply, :ok}, :local}] = effects

    {effects2, notifys} = Effects.add_reply(%{}, :ok, {:notify, :c1, me}, [], %{})
    assert effects2 == []
    assert %{^me => [c1: :ok]} = notifys
    assert [{:notify, _}] = Effects.make_notify_effects(notifys, [])
  end
end
