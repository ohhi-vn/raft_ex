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

  test "recovers registered names from an existing DETS file" do
    suffix = System.unique_integer([:positive])
    data_dir = RaftEx.TestSupport.tmp_dir("log_ets_recovery")

    names = %{
      log_ets: :"log_ets_recovery_#{suffix}",
      open_mem_tbls: :"open_mem_tbls_recovery_#{suffix}",
      directory: :"directory_recovery_#{suffix}",
      directory_rev: :"directory_rev_recovery_#{suffix}"
    }

    dets_file = Path.join(data_dir, "names.dets") |> String.to_charlist()
    table_name = names.directory_rev
    {:ok, ^table_name} = :dets.open_file(table_name, file: dets_file, access: :read_write)
    :ok = :dets.insert(names.directory_rev, {:recovered_server, "recovered_uid"})
    :ok = :dets.close(names.directory_rev)

    start_supervised!(%{
      id: names.log_ets,
      start: {LogEts, :start_link, [%{names: names, data_dir: data_dir}]}
    })

    assert :ets.lookup(names.directory, "recovered_uid") ==
             [{"recovered_uid", nil, nil, :recovered_server, nil}]
  end
end
