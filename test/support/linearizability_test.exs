defmodule RaftEx.TestSupport.LinearizabilityTest do
  use ExUnit.Case, async: true

  alias RaftEx.TestSupport.Linearizability

  test "valid sequential history passes" do
    h = Linearizability.new(0)
    {h, w1} = Linearizability.invoke(h, :write, 5)
    h = Linearizability.complete(h, w1, 5, :ok)
    {h, r1} = Linearizability.invoke(h, :read, nil)
    h = Linearizability.complete(h, r1, 5, :ok)
    assert Linearizability.verify(h) == :ok
  end

  test "stale read fails" do
    h = Linearizability.new(0)
    {h, w1} = Linearizability.invoke(h, :write, 5)
    h = Linearizability.complete(h, w1, 5, :ok)
    {h, r1} = Linearizability.invoke(h, :read, nil)
    h = Linearizability.complete(h, r1, 0, :ok)
    assert {:error, _} = Linearizability.verify(h)
  end
end
