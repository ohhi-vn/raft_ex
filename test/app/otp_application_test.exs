defmodule RaftEx.OTPApplicationTest do
  use ExUnit.Case, async: true

  alias RaftEx.OTPApplication

  describe "stop/1" do
    test "is a no-op that always returns :ok" do
      assert OTPApplication.stop(:normal) == :ok
    end

    test "ignores an arbitrary stop state" do
      assert OTPApplication.stop({:some_state, 123}) == :ok
    end
  end
end
