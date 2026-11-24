defmodule RaftEx.Domain.Plug do
  @moduledoc """
  Behaviour for RaftEx plugs, compatible with Plug conventions.
  """

  alias RaftEx.Domain.Conn

  @callback init(opts :: term()) :: term()
  @callback call(conn :: Conn.t(), opts :: term()) :: Conn.t()

  @doc "Default init returns opts unchanged."
  def init(opts), do: opts

  @doc "Helper to run a plug module with initialized opts."
  @spec call(module(), Conn.t(), term()) :: Conn.t()
  def call(mod, conn, opts) when is_atom(mod) do
    mod.call(conn, opts)
  end
end
