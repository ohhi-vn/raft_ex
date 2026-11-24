defmodule RaftEx.TestSupport do
  @moduledoc """
  Shared helpers for RaftEx tests.
  """

  @doc """
  Creates a unique temporary directory and registers `on_exit` cleanup.
  """
  def tmp_dir(prefix \\ "raft_ex_test") do
    dir = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(dir) end)

    dir
  end
end
