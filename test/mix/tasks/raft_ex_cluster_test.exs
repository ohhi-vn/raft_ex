defmodule RaftEx.Mix.Tasks.RaftExClusterTestShell do
  @moduledoc false
  @behaviour Mix.Shell

  def info(message), do: emit(:info, message)
  def error(message), do: emit(:error, message)
  def warn(message), do: emit(:warn, message)
  def prompt(message), do: emit(:prompt, message)
  def yes?(message), do: emit(:yes, message)
  def cmd(command, opts \\ []), do: emit(:cmd, {command, opts})

  defp emit(kind, message) do
    case :persistent_term.get(:raft_ex_cluster_test_shell, nil) do
      nil -> :ok
      pid -> send(pid, {:mix_shell, kind, List.wrap(message)})
    end
  end
end

defmodule RaftEx.Mix.Tasks.RaftExClusterTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.RaftEx.Cluster

  setup do
    original = Mix.shell()
    :persistent_term.put(:raft_ex_cluster_test_shell, self())
    Mix.shell(RaftEx.Mix.Tasks.RaftExClusterTestShell)

    on_exit(fn ->
      Mix.shell(original)
      :persistent_term.erase(:raft_ex_cluster_test_shell)
    end)

    # `cmd_connect/2` sets the cookie before starting distribution, so a live
    # node is required. Use a short name because long names are not resolvable
    # in every test environment.
    started_node? =
      if Node.alive?() do
        false
      else
        {:ok, _pid} =
          Node.start(
            :"facade_mix_#{System.unique_integer([:positive])}@localhost",
            :shortnames
          )

        true
      end

    if started_node?, do: Node.set_cookie(:raft_ex)

    on_exit(fn ->
      if started_node? and Node.alive?(), do: Node.stop()
    end)

    :ok
  end

  test "unknown command prints usage" do
    Cluster.run(["unknown"])
    assert_receive {:mix_shell, :info, [message]}, 10_000
    assert message =~ "Manage RaftEx distributed clusters"
  end

  test "status produces output" do
    Cluster.run(["status"])
    assert_receive {:mix_shell, _kind, [message]}, 10_000
    assert is_binary(message)
  end

  test "leader reports an unknown cluster" do
    Cluster.run(["leader", "unknown_cluster"])
    assert_receive {:mix_shell, :error, [message]}, 10_000
    assert message =~ "Could not find leader"
  end

  test "accepts options" do
    Cluster.run(["--cookie", "test_cookie", "--timeout", "100", "status"])
    assert_receive {:mix_shell, _kind, [_message]}, 10_000
  end

  test "connect accepts a short seed name and warns when no seed connects" do
    seed = "facade_seed_#{System.unique_integer([:positive])}"

    Cluster.run(["connect", seed])

    assert_receive {:mix_shell, :warn, [message]}, 10_000
    assert message =~ "No seed nodes connected"

    assert_receive {:mix_shell, :info, [message]}, 10_000
    assert message =~ "Cluster nodes:"
  end

  test "connect accepts fully qualified seed names" do
    Cluster.run(["--timeout", "100", "connect", "facade_seed@127.0.0.1"])

    assert_receive {:mix_shell, :warn, [message]}, 10_000
    assert message =~ "No seed nodes connected"

    assert_receive {:mix_shell, :info, [message]}, 10_000
    assert message =~ "Cluster nodes:"
  end

  test "connect reports reachable seeds" do
    case Node.list() do
      [peer | _] ->
        Cluster.run(["connect", Atom.to_string(peer)])

        assert_receive {:mix_shell, :info, [message]}, 10_000
        assert message =~ "Connected to:"

      [] ->
        # No peer to connect to in this VM; the warning path is covered above.
        :ok
    end
  end
end
