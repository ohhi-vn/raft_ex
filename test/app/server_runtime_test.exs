defmodule RaftEx.App.ServerRuntimeTest do
  use ExUnit.Case, async: true

  alias RaftEx.App.ServerRuntime

  defp state do
    %{
      conf: %{name: :test_system},
      server_state: %{persisted_last_applied: 0, last_applied: 0},
      election_timeout_set: false
    }
  end

  describe "gen_statem boilerplate callbacks" do
    test "callback_mode and init" do
      assert ServerRuntime.callback_mode() == [:state_functions, :state_enter]

      config = %{id: {:server, node()}, cluster_name: :test}

      assert {:ok, :post_init, ^config, [{:next_event, :internal, :go}]} =
               ServerRuntime.init(config)
    end

    test "post_init enter keeps the state" do
      s = state()
      assert {:keep_state, ^s, []} = ServerRuntime.post_init(:enter, :old, s)
    end

    test "recover postpones unknown events" do
      s = state()
      assert {:keep_state, ^s, [{:postpone, true}]} = ServerRuntime.recover(:info, :unknown, s)
    end

    test "pass-through states keep the state" do
      s = state()

      for fun <- [
            &ServerRuntime.terminating_leader/3,
            &ServerRuntime.terminating_follower/3
          ] do
        assert {:keep_state, ^s, []} = fun.(:enter, :old, s)
        assert {:keep_state, ^s, []} = fun.(:info, :anything, s)
      end

      # await_condition arms a bounded deadline on enter so a waiter can
      # never park the server forever (task 5.1).
      assert {:keep_state, _s, [{:state_timeout, _, :await_condition_timeout}]} =
               ServerRuntime.await_condition(:enter, :old, s)

      assert {:keep_state, ^s, []} = ServerRuntime.await_condition(:info, :anything, s)
    end

    test "receive_snapshot arms a transfer timeout on enter" do
      s = state()

      assert {:keep_state, _s, [{:state_timeout, _, :snapshot_transfer_timeout}]} =
               ServerRuntime.receive_snapshot(:enter, :old, s)

      # An unexpected message aborts the (absent) partial transfer and leaves
      # the receiving state for follower.
      assert {:next_state, :follower, s1, _actions} =
               ServerRuntime.receive_snapshot(:info, :anything, s)

      assert is_nil(s1.server_state.snapshot_state)
    end

    test "handle_event falls back to next_state" do
      s = state()

      assert {:next_state, :follower, ^s} =
               ServerRuntime.handle_event(:info, :unknown, :follower, s)
    end

    test "terminate and code_change" do
      assert :ok = ServerRuntime.terminate(:normal, :follower, state())
      assert :ok = ServerRuntime.terminate(:normal, :follower, %{})
      s = state()
      assert {:ok, :follower, ^s} = ServerRuntime.code_change(1, :follower, s, [])
    end
  end

  describe "state callbacks with unknown messages" do
    test "leader keeps the state" do
      assert {:keep_state, _state, _actions} = ServerRuntime.leader(:info, :unknown, state())
    end

    test "follower keeps the state" do
      assert {:keep_state, _state, _actions} = ServerRuntime.follower(:info, :unknown, state())
    end

    test "pre_vote keeps the state for unknown messages" do
      assert {:keep_state, _state, _actions} =
               ServerRuntime.pre_vote(:info, :unknown, state())
    end

    test "candidate keeps the state for unknown messages" do
      assert {:keep_state, _state, _actions} =
               ServerRuntime.candidate(:info, :unknown, state())
    end
  end

  describe "public query and call entry points" do
    test "leader queries forward options and report a missing server" do
      missing = :"missing_query_#{System.unique_integer([:positive])}"

      assert {:error, :noproc} =
               ServerRuntime.query(missing, fn s -> s end, :leader, %{option: true}, 100)
    end

    test "cast_command and query local variants reach the state machine call" do
      missing = :"missing_variants_#{System.unique_integer([:positive])}"

      assert {:error, :noproc} =
               ServerRuntime.query(missing, fn s -> s end, :local, %{option: true}, 100)

      assert {:error, :noproc} =
               ServerRuntime.query(missing, fn s -> s end, :consistent, %{}, 100)

      assert {:error, :noproc} =
               ServerRuntime.query(missing, :aux, :consistent_aux, %{}, 100)

      assert :ok = ServerRuntime.cast_command(missing, :cmd)
    end
  end
end
