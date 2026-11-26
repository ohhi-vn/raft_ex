defmodule RaftEx.Domain.InvariantTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Invariant

  test "valid indices pass" do
    assert :ok =
             Invariant.check(%{
               snapshot_index: 5,
               wal_persisted_index: 10,
               log_last_index: 10,
               commit_index: 8,
               last_applied: 8
             })
  end

  test "snapshot after wal fails" do
    assert {:error, {:snapshot_after_wal, _, _}} =
             Invariant.check(%{snapshot_index: 10, wal_persisted_index: 5, log_last_index: 10})
  end

  test "wal after log fails" do
    assert {:error, {:wal_after_log, _, _}} =
             Invariant.check(%{wal_persisted_index: 10, log_last_index: 5})
  end

  test "commit after log fails" do
    assert {:error, {:commit_after_log, _, _}} =
             Invariant.check(%{commit_index: 10, log_last_index: 5})
  end

  test "applied after commit fails" do
    assert {:error, {:applied_after_commit, _, _}} =
             Invariant.check(%{last_applied: 10, commit_index: 5, log_last_index: 10})
  end

  test "snapshot after applied fails" do
    assert {:error, {:snapshot_after_applied, _, _}} =
             Invariant.check(%{
               snapshot_index: 10,
               wal_persisted_index: 10,
               log_last_index: 10,
               commit_index: 10,
               last_applied: 5
             })
  end

  test "check! raises in test mode" do
    assert_raise RaftEx.Domain.Invariant.Violation, fn ->
      Invariant.check!(%{commit_index: 10, log_last_index: 5})
    end
  end

  test "mode is :test under test_helper" do
    assert Invariant.mode() == :test
  end
end
