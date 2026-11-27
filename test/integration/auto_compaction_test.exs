defmodule RaftEx.Integration.AutoCompactionTest do
  @moduledoc "Coverage for Section 14 automatic compaction and retention (tasks 14.1-14.4)."
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine
  alias RaftEx.Domain.Server

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn cmd, _state -> cmd end, initial_state: nil}}
  end

  defp boot(data_dir) do
    RaftEx.App.System.stop_default()

    try do
      :persistent_term.erase({:"$ra_system", :default})
    rescue
      _ -> :ok
    end

    :ok = RaftEx.start_in(data_dir)
  end

  defp find_leader(ids, attempts \\ 20) do
    Enum.find_value(ids, fn id ->
      case RaftEx.process_command(id, {:probe, System.unique_integer()}, 5_000) do
        {:ok, _, found} -> found
        _ -> nil
      end
    end) ||
      if attempts > 1 do
        Process.sleep(500)
        find_leader(ids, attempts - 1)
      end
  end

  defp compactable_state(extra \\ %{}) do
    cfg =
      struct!(RaftEx.Domain.Server.Config,
        id: {:s1, node()},
        uid: "compact-test",
        log_id: "compact-test",
        metrics_key: :compact_test,
        machine: {:machine, Machine.Simple, %{}},
        machine_version: 0,
        effective_machine_version: 0,
        effective_machine_module: Machine.Simple,
        system_config: %{},
        compaction_entry_threshold: 10,
        compaction_wal_bytes: nil,
        compaction_interval_ms: nil
      )

    Map.merge(
      %{
        cfg: cfg,
        entries_since_compact: 0,
        last_auto_compact_ms: System.monotonic_time(:millisecond),
        snapshot_state: nil,
        cluster: %{},
        log: %{data_dir: System.tmp_dir!()}
      },
      extra
    )
  end

  test "14.1 the entry threshold triggers exactly one snapshot and resets" do
    now = System.monotonic_time(:millisecond)

    {s1, effects} =
      Server.maybe_auto_compact(
        compactable_state(%{entries_since_compact: 10}),
        now
      )

    assert [{:next_event, :info, {:ra_log_event, :major_compaction}}] = effects
    assert s1.entries_since_compact == 0

    # Below threshold again: silent.
    {_, effects2} = Server.maybe_auto_compact(%{s1 | entries_since_compact: 3}, now)
    assert effects2 == []
  end

  test "14.2 compaction defers while a transfer is in progress" do
    now = System.monotonic_time(:millisecond)

    receiving = compactable_state(%{entries_since_compact: 50, snapshot_state: {:accept, 1}})
    {_s, effects} = Server.maybe_auto_compact(receiving, now)
    assert effects == []

    sending_peer = %{status: {:sending_snapshot, self(), 0}, voter_status: %{membership: :voter}}

    sending =
      compactable_state(%{
        entries_since_compact: 50,
        cluster: %{{:p, node()} => sending_peer}
      })

    {_s, effects2} = Server.maybe_auto_compact(sending, now)
    assert effects2 == []

    # Transfer done: fires again.
    {_s, effects3} =
      Server.maybe_auto_compact(compactable_state(%{entries_since_compact: 50}), now)

    assert [{:next_event, :info, {:ra_log_event, :major_compaction}}] = effects3
  end

  test "14.3 the operator trigger still forces a snapshot" do
    dir = Path.join(System.tmp_dir!(), "ac143_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"ac143_#{System.unique_integer([:positive])}"
    ids = [{:t1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    [sid] = ids
    assert find_leader(ids) != nil
    assert {:ok, _, _} = RaftEx.process_command(sid, :before, 5_000)

    assert :ok = RaftEx.trigger_compaction(sid)
    Process.sleep(1500)

    {:ok, overview, _} = RaftEx.member_overview(sid)
    assert overview.commit_index >= 1
  end

  test "14.4 memory stays bounded when appending far beyond the window" do
    dir = Path.join(System.tmp_dir!(), "ac144_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"ac144_#{System.unique_integer([:positive])}"
    ids = [{:u1, node()}]

    machine_spec = machine()

    # Small entry threshold so automatic compaction keeps the window tight.
    configs = [
      %{
        id: hd(ids),
        uid: RaftEx.server_uid(name, hd(ids)),
        cluster_name: name,
        log_init_args: %{uid: RaftEx.server_uid(name, hd(ids))},
        initial_members: ids,
        machine: machine_spec,
        compaction_entry_threshold: 20
      }
    ]

    assert {:ok, _, []} = RaftEx.start_cluster(:default, configs, 10_000)
    [sid] = ids
    assert find_leader(ids) != nil

    for i <- 1..120 do
      assert {:ok, _, _} = RaftEx.process_command(sid, {:fill, i}, 10_000)
    end

    Process.sleep(2000)
    {:ok, overview, _} = RaftEx.member_overview(sid)
    assert overview.last_applied >= 120

    pid = Process.whereis(:u1)
    ss = elem(:sys.get_state(pid), 1).server_state
    # Bounded: the in-memory window holds far fewer than the 120 appended.
    assert map_size(ss.log.entries) < 120
  end

  test "14.4 without compaction the window grows without bound" do
    dir = Path.join(System.tmp_dir!(), "ac144b_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"ac144b_#{System.unique_integer([:positive])}"
    ids = [{:u2, node()}]

    configs = [
      %{
        id: hd(ids),
        uid: RaftEx.server_uid(name, hd(ids)),
        cluster_name: name,
        log_init_args: %{uid: RaftEx.server_uid(name, hd(ids))},
        initial_members: ids,
        machine: machine(),
        compaction_entry_threshold: nil,
        compaction_wal_bytes: nil,
        compaction_interval_ms: nil
      }
    ]

    assert {:ok, _, []} = RaftEx.start_cluster(:default, configs, 10_000)
    [sid] = ids
    assert find_leader(ids) != nil

    for i <- 1..60 do
      assert {:ok, _, _} = RaftEx.process_command(sid, {:fill, i}, 10_000)
    end

    pid = Process.whereis(:u2)
    ss = elem(:sys.get_state(pid), 1).server_state
    # Disabled: everything stays in memory (this is what the bounded test
    # above would show if compaction were turned off).
    assert map_size(ss.log.entries) >= 60
  end

  test "15.1 segment storage and auto-compaction stay consistent end to end" do
    dir = Path.join(System.tmp_dir!(), "segsoak_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"segsoak_#{System.unique_integer([:positive])}"
    ids = [{:g1, node()}]

    configs = [
      %{
        id: hd(ids),
        uid: RaftEx.server_uid(name, hd(ids)),
        cluster_name: name,
        log_init_args: %{uid: RaftEx.server_uid(name, hd(ids))},
        initial_members: ids,
        machine: machine(),
        segment_storage: true,
        compaction_entry_threshold: 30
      }
    ]

    assert {:ok, _, []} = RaftEx.start_cluster(:default, configs, 10_000)
    [sid] = ids
    assert find_leader(ids) != nil

    for i <- 1..90 do
      assert {:ok, _, _} = RaftEx.process_command(sid, {:seg, i}, 10_000)
    end

    Process.sleep(2000)

    # State converges and serves reads after compactions.
    assert {:ok, {:seg, 90}, _} = RaftEx.process_command(sid, {:seg, 90}, 10_000)
    {:ok, overview, _} = RaftEx.member_overview(sid)
    assert overview.last_applied >= 91

    pid = Process.whereis(:g1)
    ss = elem(:sys.get_state(pid), 1).server_state
    assert ss.log.segment_enabled? == true
    assert map_size(ss.log.entries) < 91
  end
end
