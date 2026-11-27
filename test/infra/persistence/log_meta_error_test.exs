defmodule RaftEx.Infra.Persistence.LogMetaErrorTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogMeta

  test "store_sync to a missing server returns an error instead of raising" do
    missing = :"missing_meta_#{System.unique_integer([:positive])}"

    assert {:error, _} = LogMeta.store_sync(missing, "uid", :current_term, 1)
    assert {:error, _} = LogMeta.delete_sync(missing, "uid")
  end

  test "store_sync reports a failure instead of success after the store stops" do
    data_dir = RaftEx.TestSupport.tmp_dir("log_meta_error")
    name = :"log_meta_err_#{System.unique_integer([:positive])}"

    start_supervised!(
      {LogMeta, %{name: :log_meta_err, names: %{log_meta: name}, data_dir: data_dir}}
    )

    assert :ok = LogMeta.store_sync(name, "uid-1", :current_term, 1)

    :ok = stop_supervised(LogMeta)

    assert {:error, _} = LogMeta.store_sync(name, "uid-1", :current_term, 2)
    assert {:error, _} = LogMeta.delete_sync(name, "uid-1")
  end
end
