defmodule RaftEx.Mix.Tasks.RaftExClusterTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.RaftEx.Cluster

  setup do
    original = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(original) end)
    :ok
  end

  test "unknown command prints usage" do
    Cluster.run(["unknown"])
    assert_receive {:mix_shell, :info, [message]}
    assert message =~ "Manage RaftEx distributed clusters"
  end

  test "status produces output" do
    Cluster.run(["status"])
    assert_receive {:mix_shell, _kind, [message]}
    assert is_binary(message)
  end

  test "leader reports an unknown cluster" do
    Cluster.run(["leader", "unknown_cluster"])
    assert_receive {:mix_shell, :error, [message]}
    assert message =~ "Could not find leader"
  end

  test "accepts options" do
    Cluster.run(["--cookie", "test_cookie", "--timeout", "100", "status"])
    assert_receive {:mix_shell, _kind, [_message]}
  end
end
