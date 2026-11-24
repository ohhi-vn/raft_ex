defmodule RaftEx.Domain.Server.EffectsTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Server.Effects

  describe "add_reply/5" do
    test "swallows no-reply commands" do
      assert Effects.add_reply(%{from: self()}, :reply, :"$ra_no_reply", [], %{}) == {[], %{}}
    end

    test "replies directly for await_consensus" do
      from = self()
      assert {effects, notifys} = Effects.add_reply(%{from: from}, :ok, :await_consensus, [], %{})

      assert effects == [{:reply, from, {:wrap_reply, :ok}}]
      assert notifys == %{}
    end

    test "supports reply_from routing options" do
      from = self()

      for {opts, expected} <- [
            {%{reply_from: :local}, :local},
            {%{reply_from: {:member, :s2}}, {:member, :s2}},
            {%{other: 1}, :leader}
          ] do
        {effects, _} = Effects.add_reply(%{from: from}, :ok, {:await_consensus, opts}, [], %{})
        assert effects == [{:reply, from, {:wrap_reply, :ok}, expected}]
      end
    end

    test "batches pipeline notifications by pid" do
      pid = self()
      assert {effects, notifys} = Effects.add_reply(%{}, :a, {:notify, 1, pid}, [], %{})

      assert effects == []
      assert notifys == %{pid => [{1, :a}]}

      {effects, notifys} = Effects.add_reply(%{}, :b, {:notify, 2, pid}, effects, notifys)
      assert notifys == %{pid => [{2, :b}, {1, :a}]}
    end

    test "discards unknown reply modes" do
      assert Effects.add_reply(%{from: self()}, :reply, :noreply, [:existing], %{}) ==
               {[:existing], %{}}

      assert Effects.add_reply(%{}, :reply, :unknown, [:existing], %{}) == {[:existing], %{}}
    end
  end

  describe "append_machine_effects/2" do
    test "handles empty, single and multiple effects" do
      assert Effects.append_machine_effects([], [:prior]) == [:prior]
      assert Effects.append_machine_effects([:one], [:prior]) == [:one, :prior]
      assert Effects.append_machine_effects([:one, :two], [:prior]) == [[:one, :two], :prior]
      assert Effects.append_machine_effects(:single, []) == [:single]
    end
  end

  describe "make_notify_effects/2" do
    test "wraps a non-empty notification map" do
      notifys = %{self() => [{1, :ok}]}
      assert Effects.make_notify_effects(notifys, [:prior]) == [{:notify, notifys}, :prior]
    end

    test "leaves effects unchanged for an empty map" do
      assert Effects.make_notify_effects(%{}, [:prior]) == [:prior]
    end
  end

  describe "append_error_reply/3" do
    test "emits an error reply when a from field is present" do
      from = self()
      cmd = {:usr, %{from: from}, :command, :noreply}

      assert Effects.append_error_reply(cmd, :bad_request, [:prior]) == [
               {:reply, from, {:error, :bad_request}},
               :prior
             ]
    end

    test "leaves effects unchanged for other commands" do
      assert Effects.append_error_reply({:usr, %{}, :cmd, :noreply}, :error, [:prior]) == [:prior]
      assert Effects.append_error_reply(:not_a_tuple, :error, [:prior]) == [:prior]
    end
  end

  describe "after_log_append_reply/4" do
    test "emits an index/term acknowledgement" do
      from = self()
      cmd = {:usr, %{from: from}, :command, :after_log_append}

      assert Effects.after_log_append_reply(cmd, 7, 2, [:prior]) == [
               {:reply, from, {:wrap_reply, {7, 2}}},
               :prior
             ]
    end

    test "leaves effects unchanged for other reply modes" do
      cmd = {:usr, %{from: self()}, :command, :noreply}
      assert Effects.after_log_append_reply(cmd, 7, 2, [:prior]) == [:prior]
      assert Effects.after_log_append_reply(:not_a_tuple, 7, 2, [:prior]) == [:prior]
    end
  end
end
