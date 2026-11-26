defmodule RaftEx.TestSupport.FaultInjection do
  @moduledoc """
  Fault-injection primitives for RaftEx chaos tests.

  All helpers are best-effort and clean up after themselves via `on_exit`
  where applicable. They target in-BEAM (single-node) clusters.
  """

  @doc "Crash a server process by id (atom name). Returns :ok."
  def crash(server_name) when is_atom(server_name) do
    case Process.whereis(server_name) do
      nil -> :ok
      pid -> Process.exit(pid, :kill)
    end

    :ok
  end

  @doc "Make a data directory read-only to simulate a disk failure."
  def disk_failure_readonly(dir) do
    File.chmod!(dir, 0o555)
    :ok
  end

  @doc "Restore a data directory to writable."
  def disk_failure_restore(dir) do
    File.chmod!(dir, 0o755)
    :ok
  end

  @doc "Delay execution by `ms` milliseconds (simulates slow RPC)."
  def delay(ms) when is_integer(ms) and ms >= 0 do
    Process.sleep(ms)
    :ok
  end

  @doc "Suspend a server process to simulate an unresponsive (partitioned) node."
  def partition(server_name) when is_atom(server_name) do
    case Process.whereis(server_name) do
      nil -> :ok
      pid -> :sys.suspend(pid)
    end

    :ok
  end

  @doc "Resume a previously suspended server process (heal the partition)."
  def heal(server_name) when is_atom(server_name) do
    case Process.whereis(server_name) do
      nil -> :ok
      pid -> :sys.resume(pid)
    end

    :ok
  end

  @doc "Return the chaos seed from env (CHAOS_SEED) or ExUnit's seed."
  def seed do
    case System.get_env("CHAOS_SEED") do
      nil -> :erlang.phash2(:os.system_time(), 1_000_000)
      v -> String.to_integer(v)
    end
  end

  @doc "Dump a term to a file under the system temp dir for replay/debugging."
  def dump_artifact(name, term) do
    path = Path.join(System.tmp_dir!(), "chaos_#{name}_#{System.system_time(:millisecond)}.term")
    File.write!(path, :erlang.term_to_binary(term))
    path
  end

  @doc "Run `fun` with a temporary env override, restoring afterwards."
  def with_env(key, value, fun) do
    prev = Application.get_env(:raft_ex, key)
    Application.put_env(:raft_ex, key, value)

    try do
      fun.()
    after
      if prev == nil do
        Application.delete_env(:raft_ex, key)
      else
        Application.put_env(:raft_ex, key, prev)
      end
    end
  end
end
