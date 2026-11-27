defmodule RaftEx.ServerUidTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  test "server_uid is deterministic per server and distinct across servers" do
    assert RaftEx.server_uid(:my_cluster, {:s1, node()}) ==
             RaftEx.server_uid("my_cluster", {:s1, node()})

    assert RaftEx.server_uid(:my_cluster, {:s1, node()}) !=
             RaftEx.server_uid(:my_cluster, {:s2, node()})

    assert RaftEx.server_uid(:a, {:s1, node()}) !=
             RaftEx.server_uid(:b, {:s1, node()})
  end

  test "explicit uid wins over the derived uid" do
    dir = Path.join(System.tmp_dir!(), "uid_explicit_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    RaftEx.App.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    :ok = RaftEx.start_in(dir)

    on_exit(fn ->
      RaftEx.App.System.stop_default()
      :persistent_term.erase({:"$ra_system", :default})
    end)

    machine = {:machine, Machine.Simple, %{}}
    id = {:explicit_uid_server, node()}

    config = %{
      id: id,
      uid: "EXPLICITUID1234",
      cluster_name: :explicit_uid_cluster,
      log_init_args: %{uid: "EXPLICITUID1234"},
      initial_members: [id],
      machine: machine
    }

    assert :ok = RaftEx.start_server(:default, config)
  end

  test "duplicate explicit uids are rejected before starting anything" do
    machine = {:machine, Machine.Simple, %{}}

    ids = [{:dup1, node()}, {:dup2, node()}]

    configs =
      Enum.map(ids, fn id ->
        %{
          id: id,
          uid: "DUPLICATEUID12",
          cluster_name: :dup_uid_cluster,
          log_init_args: %{uid: "DUPLICATEUID12"},
          initial_members: ids,
          machine: machine
        }
      end)

    assert {:error, {:uid_collision, "DUPLICATEUID12", _}} =
             RaftEx.start_cluster(:default, configs, 1_000)
  end
end
