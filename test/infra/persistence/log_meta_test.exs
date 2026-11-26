defmodule RaftEx.Infra.Persistence.LogMetaTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogMeta

  setup do
    data_dir = RaftEx.TestSupport.tmp_dir("log_meta_test")
    name = :"log_meta_test_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogMeta, %{name: :log_meta_test, names: %{log_meta: name}, data_dir: data_dir}}
    )

    Process.put(:log_meta_test_name, name)
    Process.put(:log_meta_test_dir, data_dir)
    :ok
  end

  test "store and fetch fields" do
    name = name()
    uid = "meta-uid"

    assert :ok = LogMeta.store(name, uid, :current_term, 2)
    assert :ok = LogMeta.store_sync(name, uid, :voted_for, {:server1, node()})
    assert :ok = LogMeta.store_sync(name, uid, :last_applied, 5)

    assert LogMeta.fetch(name, uid, :current_term) == 2
    assert LogMeta.fetch(name, uid, :voted_for) == {:server1, node()}
    assert LogMeta.fetch(name, uid, :last_applied) == 5
  end

  test "concurrent store_sync callers all persist their values" do
    name = name()
    uids = Enum.map(1..20, &"concurrent-uid-#{&1}")

    tasks =
      Enum.map(uids, fn uid ->
        Task.async(fn ->
          assert :ok = LogMeta.store_sync(name, uid, :current_term, 7)
          assert :ok = LogMeta.store_sync(name, uid, :last_applied, 9)
        end)
      end)

    Task.await_many(tasks, :infinity)

    for uid <- uids do
      assert LogMeta.fetch(name, uid, :current_term) == 7
      assert LogMeta.fetch(name, uid, :last_applied) == 9
    end
  end

  test "persisted values survive a restart" do
    name = name()
    dir = Process.get(:log_meta_test_dir)
    uid = "restart-uid"

    assert :ok = LogMeta.store_sync(name, uid, :current_term, 11)
    assert :ok = LogMeta.store_sync(name, uid, :voted_for, {:leader, node()})

    :ok = stop_supervised(LogMeta)

    # Reopen the same data directory under a fresh process name.
    new_name = :"log_meta_reopened_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogMeta, %{name: :log_meta_reopened, names: %{log_meta: new_name}, data_dir: dir}}
    )

    assert LogMeta.fetch(new_name, uid, :current_term) == 11
    assert LogMeta.fetch(new_name, uid, :voted_for) == {:leader, node()}

    :ok = stop_supervised(LogMeta)
  end

  test "fetch with a default" do
    assert LogMeta.fetch(name(), "unknown-uid", :current_term, 42) == 42
  end

  test "delete and delete_sync" do
    name = name()
    uid = "delete-uid"
    LogMeta.store_sync(name, uid, :current_term, 3)

    assert :ok = LogMeta.delete(name, uid)
    assert eventually(fn -> LogMeta.fetch(name, uid, :current_term) == nil end)

    LogMeta.store_sync(name, uid, :current_term, 4)
    assert :ok = LogMeta.delete_sync(name, uid)
    assert LogMeta.fetch(name, uid, :current_term) == nil
  end

  test "fetch on a missing table returns nil" do
    assert LogMeta.fetch(
             :"missing_meta_#{System.unique_integer([:positive])}",
             "uid",
             :current_term
           ) ==
             nil
  end

  defp name, do: Process.get(:log_meta_test_name)

  defp eventually(_fun, 0), do: false

  defp eventually(fun, tries \\ 20) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, tries - 1)
    end
  end
end
