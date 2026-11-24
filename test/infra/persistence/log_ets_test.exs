defmodule RaftEx.Infra.Persistence.LogEtsTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Persistence.LogEts

  test "starts the ETS tables used by the log layer" do
    suffix = System.unique_integer([:positive])

    names = %{
      log_ets: :"log_ets_test_#{suffix}",
      open_mem_tbls: :"open_mem_tbls_test_#{suffix}",
      directory: :"directory_test_#{suffix}",
      directory_rev: :"directory_rev_test_#{suffix}"
    }

    config = %{names: names, data_dir: RaftEx.TestSupport.tmp_dir("log_ets_test")}
    assert {:ok, _pid} = LogEts.start_link(config)

    assert :ets.info(names.log_ets) != :undefined
    assert :ets.info(names.open_mem_tbls) != :undefined
    assert :ets.info(names.directory) != :undefined
    assert is_list(:dets.info(names.directory_rev))
  end
end
