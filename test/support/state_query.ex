defmodule RaftEx.TestSupport.StateQuery do
  @moduledoc """
  A named identity query for cross-node `RaftEx.local_query/3` calls.

  Anonymous functions defined inside a test module cannot be evaluated on
  `:peer` nodes because test modules are not on their code path. This module is
  compiled into the test ebin directory, so peers can resolve the fun.
  """

  @doc "Returns an identity query function."
  def fun, do: fn state -> state end
end
