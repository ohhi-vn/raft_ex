defmodule RaftEx.App.RecoveryTest do
  use ExUnit.Case, async: false

  alias RaftEx.App.Recovery
  alias RaftEx.App.System, as: SystemConfig

  defmodule RecoveryProbe do
    def recover(system, arg, parent), do: send(parent, {:recovered, system, arg})
  end

  describe "init/1" do
    test "does nothing when no recovery strategy is configured" do
      name = :"recovery_none_#{System.unique_integer([:positive])}"
      assert {:ok, pid} = Recovery.start_link(name)
      assert Process.alive?(pid)
    end

    test "invokes a configured mfa strategy" do
      name = :"recovery_mfa_#{System.unique_integer([:positive])}"
      parent = self()

      SystemConfig.store(%{
        name: name,
        server_recovery_strategy: {RecoveryProbe, :recover, [:arg, parent]}
      })

      on_exit(fn -> :persistent_term.erase({:"$ra_system", name}) end)

      assert {:ok, _pid} = Recovery.start_link(name)
      assert_receive {:recovered, ^name, :arg}
    end
  end
end
