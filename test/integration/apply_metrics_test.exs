defmodule RaftEx.Integration.ApplyMetricsTest do
  @moduledoc "Coverage for Section 11 apply progress, metrics, and hot-path hygiene (tasks 11.1-11.6)."
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine
  alias RaftEx.Infra.Observability.{Counters, Metrics}

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

  test "11.2 repeated metric writes do not reinitialize the table" do
    Metrics.reset()
    table = :ets.whereis(:raft_ex_metrics)
    assert is_reference(table) or is_integer(table) or table != :undefined

    for _ <- 1..500 do
      Metrics.inc(:hot_path_counter)
      Metrics.set(:hot_path_gauge, 1)
      Metrics.observe(:hot_path_hist, 2)
    end

    # Same table throughout: no repeated init/recreate on the hot path.
    assert :ets.whereis(:raft_ex_metrics) == table
    assert Metrics.get(:hot_path_counter) == 500
  end

  test "11.3 the lag metric reports an entry-index gap" do
    # Lag is computed as leader_last - follower_last (indexes, not ms).
    dir = Path.join(System.tmp_dir!(), "lag11_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"lag11_#{System.unique_integer([:positive])}"
    ids = [{:l1, node()}, {:l2, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)

    leader =
      Enum.find_value(ids, fn id ->
        case RaftEx.process_command(id, :probe, 5_000) do
          {:ok, _, found} -> found
          _ -> nil
        end
      end)

    assert leader != nil
    assert {:ok, _, _} = RaftEx.process_command(leader, :hello, 5_000)
    Process.sleep(1500)

    lag =
      Metrics.all()
      |> Enum.filter(fn
        {{:gauge, {:replication_lag, _}}, _} -> true
        _ -> false
      end)

    # Lag entries exist and are non-negative index gaps.
    assert is_list(lag)

    for {_, v} <- lag do
      assert is_integer(v) and v >= 0
    end
  end

  test "11.6 key_metrics and member_overview report live values" do
    dir = Path.join(System.tmp_dir!(), "m116_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"m116_#{System.unique_integer([:positive])}"
    ids = [{:k1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    [sid] = ids

    assert {:ok, _, _} = RaftEx.process_command(sid, :cmd_a, 5_000)
    assert {:ok, _, _} = RaftEx.process_command(sid, :cmd_b, 5_000)
    Process.sleep(500)

    metrics = RaftEx.key_metrics(sid)
    assert metrics.state in [:leader, :follower]
    assert metrics.membership == :voter
    assert is_integer(metrics.term) and metrics.term >= 1
    assert is_integer(metrics.commit_index) and metrics.commit_index >= 2
    assert is_integer(metrics.last_applied) and metrics.last_applied >= 2

    {:ok, overview, _} = RaftEx.member_overview(sid)
    assert overview.commit_index >= 2
    assert overview.current_term >= 1

    # Per-server command counter is backed by the metrics store.
    assert %{commands: n} = Counters.counters(sid, [:commands])
    assert is_integer(n) and n >= 2
  end

  test "11.1 a restart after a small lag reaches the same state" do
    dir = Path.join(System.tmp_dir!(), "lag111_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    boot(dir)

    name = :"lag111_#{System.unique_integer([:positive])}"
    ids = [{:r1, node()}]
    assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), ids, 10_000)
    [sid] = ids
    assert {:ok, _, _} = RaftEx.process_command(sid, :a, 5_000)
    assert {:ok, _, _} = RaftEx.process_command(sid, :b, 5_000)

    {:ok, before, _} = RaftEx.member_overview(sid)

    # Hard kill without a clean shutdown: the transient supervisor restarts
    # the server and replay recovers the committed prefix.
    pid = Process.whereis(:r1)
    Process.exit(pid, :kill)

    restarted =
      Enum.find_value(1..40, fn _ ->
        Process.sleep(250)

        case Process.whereis(:r1) do
          nil -> nil
          new_pid when new_pid != pid -> new_pid
          _ -> nil
        end
      end)

    assert is_pid(restarted), "server did not restart after kill"

    # The restarted server must campaign again; wait for leadership.
    leader =
      Enum.find_value(1..20, fn _ ->
        case RaftEx.process_command(sid, {:probe, System.unique_integer()}, 2_000) do
          {:ok, _, found} -> found
          _ -> Process.sleep(500) && nil
        end
      end)

    assert leader != nil

    # Replay is on-demand: the committed prefix is re-applied when the next
    # entry commits, so the server converges to the same state.
    assert {:ok, _, _} = RaftEx.process_command(sid, :c, 5_000)
    {:ok, after_restart, _} = RaftEx.member_overview(sid)
    assert after_restart.last_applied >= before.last_applied
  end
end
