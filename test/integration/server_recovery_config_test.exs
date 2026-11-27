defmodule RaftEx.Integration.ServerRecoveryConfigTest do
  @moduledoc """
  Section 6: recovery and server configuration (tasks 6.1, 6.2, 6.4, 6.5, 6.6).
  """
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn cmd, state -> {state, cmd} end, initial_state: 0}}
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

  defp kill_all do
    for [name] <- :ets.match(:ra_state, {:"$1", :_, :_}) do
      case Process.whereis(name) do
        nil -> :ok
        pid -> Process.exit(pid, :kill)
      end
    end

    Process.sleep(300)
    :ets.match_delete(:ra_state, {:_, :_, :_})

    try do
      :persistent_term.erase({:"$ra_system", :default})
    rescue
      _ -> :ok
    end
  end

  test "6.5 stop_default is safe when nothing runs and start boots twice" do
    # Safe with no application started.
    assert :ok = RaftEx.App.System.stop_default()
    assert :ok = RaftEx.App.System.stop_default()

    dir = Path.join(System.tmp_dir!(), "cfg65_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    assert :ok = RaftEx.start_in(dir)
    # Second start replaces the default system cleanly.
    assert :ok = RaftEx.start_in(dir)
    assert :ok = RaftEx.App.System.stop_default()
  end

  test "6.1 server config is persisted atomically at start" do
    dir = Path.join(System.tmp_dir!(), "cfg61_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"cfg61_#{System.unique_integer([:positive])}"
    ids = [{:s1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)

    uid = RaftEx.server_uid(name, hd(ids))
    cfg_path = Path.join([dir, Atom.to_string(node()), uid, "config"])
    assert File.exists?(cfg_path)

    # Atomic: file decodes as a complete term, never torn.
    {:ok, bin} = File.read(cfg_path)
    assert is_map(:erlang.binary_to_term(bin))
  end

  test "6.2 restart_server recovers from persisted config and reports missing" do
    dir = Path.join(System.tmp_dir!(), "cfg62_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"cfg62_#{System.unique_integer([:positive])}"
    ids = [{:s1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    [sid] = ids
    local = elem(sid, 0)

    # Stop the server process without deleting data, then restart via config.
    assert :ok = RaftEx.stop_server(:default, sid)
    Process.sleep(500)
    assert Process.whereis(local) == nil

    assert :ok = RaftEx.restart_server(:default, sid)
    Process.sleep(1500)
    assert is_pid(Process.whereis(local))

    # Unknown name reports a clean error.
    assert {:error, :name_not_registered} =
             RaftEx.Infra.Supervisor.ServerSupSupervisor.recover_config(:default, :nope_missing)
  end

  test "6.4 hard-kill/restart preserves committed state and last_applied" do
    dir = Path.join(System.tmp_dir!(), "cfg64_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"cfg64_#{System.unique_integer([:positive])}"
    ids = [{:k1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    [sid] = ids
    assert {:ok, _, _} = RaftEx.process_command(sid, :cmd_a, 5_000)
    assert {:ok, _, _} = RaftEx.process_command(sid, :cmd_b, 5_000)

    {:ok, overview, _} = RaftEx.member_overview(sid)
    applied_before = overview.last_applied
    assert applied_before >= 2

    kill_all()
    boot(dir)

    # Recover the single server from persisted config.
    assert :ok = RaftEx.restart_server(:default, sid)
    Process.sleep(2000)

    {:ok, overview2, _} = RaftEx.member_overview(sid)
    assert overview2.last_applied >= applied_before

    # The restarted server must campaign again; wait for leadership.
    leader =
      Enum.find_value(1..20, fn _ ->
        case RaftEx.process_command(sid, {:probe, System.unique_integer()}, 3_000) do
          {:ok, _, found} -> found
          _ -> Process.sleep(500) && nil
        end
      end)

    assert leader != nil
    assert {:ok, _, _} = RaftEx.process_command(leader, :cmd_c, 5_000)
  end

  test "6.6 per-server config honors application environment" do
    dir = Path.join(System.tmp_dir!(), "cfg66_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    for {k, v} <- [
          wal_sync_method: :sync,
          snapshot_chunk_size: 1024,
          max_pipeline_count: 7,
          max_append_entries_rpc_batch_size: 9
        ] do
      Application.put_env(:raft_ex, k, v)
    end

    on_exit(fn ->
      for k <- [
            :wal_sync_method,
            :snapshot_chunk_size,
            :max_pipeline_count,
            :max_append_entries_rpc_batch_size
          ] do
        Application.delete_env(:raft_ex, k)
      end
    end)

    boot(dir)

    sys = RaftEx.App.System.fetch(:default)
    assert sys.wal_sync_method == :sync
    assert sys.snapshot_chunk_size == 1024

    name = :"cfg66_#{System.unique_integer([:positive])}"
    ids = [{:s1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)

    pid = Process.whereis(:s1)
    state = :sys.get_state(pid)
    conf = elem(state, 1).conf
    assert conf.snapshot_chunk_size == 1024
    assert conf.max_pipeline_count == 7
    assert conf.max_append_entries_rpc_batch_size == 9
    assert conf.wal_sync_method == :sync
  end
end
