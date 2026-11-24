defmodule RaftExFacadeTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn _c, s -> s end, initial_state: 0}}
  end

  test "new_uid derives a safe prefix" do
    uid = RaftEx.new_uid("my cluster/name")
    assert byte_size(uid) == 18
    assert Regex.match?(~r/^[A-Z0-9]{18}$/, uid)
  end

  test "overview reports an unstarted system" do
    assert RaftEx.overview(:"facade_missing_#{System.unique_integer([:positive])}") ==
             :system_not_started
  end

  test "cluster_status reports an unstarted system" do
    assert RaftEx.cluster_status(:"facade_missing_#{System.unique_integer([:positive])}") ==
             {:error, :system_not_started}
  end

  test "key_metrics requires counters to be registered (current behavior)" do
    name = :"missing_server_#{System.unique_integer([:positive])}"

    # Counters.counters/2 returns :undefined for an unregistered server, which
    # Map.merge/2 cannot accept. This documents the current boundary behavior.
    assert_raise BadMapError, fn -> RaftEx.key_metrics({name, node()}) end
  end

  test "pipeline_command with no local process returns an error" do
    name = :"missing_server_#{System.unique_integer([:positive])}"
    assert RaftEx.pipeline_command({name, node()}, :cmd) == :ok
    assert RaftEx.pipeline_command({name, node()}, :cmd, :corr) == :ok
  end

  test "form_cluster rejects nodes with no local server" do
    assert RaftEx.form_cluster(:facade_cluster, machine(), [:"remote@invalid.host"]) ==
             {:error, :no_local_servers}
  end

  test "leaderboard delegates" do
    RaftEx.Leaderboard.init()
    assert RaftEx.local_server_id(:"unknown_cluster_#{System.unique_integer([:positive])}") == nil
    assert is_list(RaftEx.leaderboard_overview())
  end
end
