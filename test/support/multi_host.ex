defmodule RaftEx.TestSupport.MultiHost do
  @moduledoc """
  Coordinator helpers for multi-host integration tests.

  Activated by the `RAFT_EX_HOSTS` env var (comma-separated hostnames). When
  unset, tests using this module skip gracefully.
  """

  @doc "Returns the list of configured hosts, or nil when unset."
  def hosts do
    case System.get_env("RAFT_EX_HOSTS") do
      nil -> nil
      "" -> nil
      v -> String.split(v, ",", trim: true)
    end
  end

  @doc "Whether multi-host testing is enabled."
  def enabled?, do: hosts() != nil

  @doc "Connect to a remote node by name."
  def connect(node_name) do
    Node.connect(node_name)
  end

  @doc "Run a function on a remote node."
  def rpc(node, mod, fun, args, timeout \\ 10_000) do
    :rpc.call(node, mod, fun, args, timeout)
  end

  @doc "Skip-friendly guard for use in setup."
  def skip_unless_enabled do
    if not enabled?() do
      {:skip, "RAFT_EX_HOSTS not set; skipping multi-host test"}
    else
      :ok
    end
  end
end
