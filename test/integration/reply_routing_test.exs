defmodule RaftEx.ReplyRoutingTest do
  @moduledoc "End-to-end coverage for Section 5: reply routing (5.3), pipeline notifications (5.4), cluster delete (5.5), and member overview (5.8)."
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  setup do
    data_dir = Path.join(System.tmp_dir!(), "raft_reply_#{System.unique_integer([:positive])}")
    File.mkdir_p!(data_dir)

    RaftEx.App.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    :ok = RaftEx.start_in(data_dir)

    on_exit(fn ->
      for [name] <- :ets.match(:ra_state, {:"$1", :_, :_}) do
        case Process.whereis(name) do
          nil -> :ok
          pid -> :gen_statem.stop(pid)
        end
      end

      :ets.match_delete(:ra_state, {:_, :_, :_})
      :persistent_term.erase({:"$ra_system", :default})
      File.rm_rf!(data_dir)
    end)

    %{data_dir: data_dir}
  end

  defp machine do
    # Fixed reply regardless of state, so assertions stay deterministic.
    {:machine, Machine.Simple,
     %{simple_fun: fn _cmd, _state -> {nil, :fixed_reply} end, initial_state: 0}}
  end

  defp single_server_cluster(name) do
    id = {:solo, node()}
    assert {:ok, [^id], []} = RaftEx.start_cluster(:default, name, machine(), [id], 10_000)
    id
  end

  test "reply_from routes the reply and identifies the replier (5.3)", %{data_dir: _} do
    name = :"reply_#{System.unique_integer([:positive])}"
    id = single_server_cluster(name)

    assert {:ok, {nil, :fixed_reply}, :local} =
             RaftEx.process_command(id, :hello, %{reply_from: :local, timeout: 5_000})

    assert {:ok, {nil, :fixed_reply}, {:member, ^id}} =
             RaftEx.process_command(id, :hello, %{reply_from: {:member, id}, timeout: 5_000})

    # The default path still reports the leader.
    assert {:ok, {nil, :fixed_reply}, ^id} = RaftEx.process_command(id, :hello, 5_000)
  end

  test "pipeline commands deliver exactly one notification (5.4)", %{data_dir: _} do
    name = :"notify_#{System.unique_integer([:positive])}"
    id = single_server_cluster(name)

    assert :ok = RaftEx.pipeline_command(id, :ping_cmd, :corr_1)

    assert_receive {:pipeline_notify, :corr_1, {nil, :fixed_reply}}, 5_000
    refute_receive {:pipeline_notify, :corr_1, _}, 500
  end

  test "member overview is answered directly (5.8)", %{data_dir: _} do
    name = :"overview_#{System.unique_integer([:positive])}"
    id = single_server_cluster(name)

    assert {:ok, overview, ^id} = RaftEx.member_overview(id, 5_000)
    assert is_map(overview) or is_list(overview)
  end

  test "delete_cluster terminates the server without a crash loop (5.5)", %{data_dir: data_dir} do
    name = :"bye_#{System.unique_integer([:positive])}"
    id = single_server_cluster(name)
    local_name = elem(id, 0)

    assert is_pid(Process.whereis(local_name))
    server_dir = Path.join([data_dir, Atom.to_string(node()), RaftEx.server_uid(name, id)])
    assert File.exists?(server_dir)

    assert {:ok, _} = RaftEx.delete_cluster([id], 10_000)

    # Termination is asynchronous w.r.t. name unregistration: poll briefly.
    gone =
      Enum.any?(1..50, fn _ ->
        Process.sleep(100)
        Process.whereis(local_name) == nil
      end)

    assert gone, "server process did not terminate after delete"
    refute File.exists?(server_dir)

    # No crash loop restarts it.
    Process.sleep(2_000)
    assert Process.whereis(local_name) == nil
    refute File.exists?(server_dir)
  end
end
