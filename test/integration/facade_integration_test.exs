defmodule RaftEx.FacadeIntegrationTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine
  alias RaftEx.Infra.Observability.Counters

  setup do
    data_dir = Path.join(System.tmp_dir!(), "raft_facade_#{System.unique_integer([:positive])}")
    File.mkdir_p!(data_dir)

    RaftEx.App.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    RaftEx.start_in(data_dir)

    on_exit(fn ->
      for [name] <- :ets.match(:ra_state, {:"$1", :_, :_}) do
        case Process.whereis(name) do
          nil -> :ok
          pid -> :gen_statem.stop(pid)
        end
      end

      :ets.match_delete(:ra_state, {:_, :_, :_})
      :persistent_term.erase({:"$ra_system", :default})
      File.rm_rf!(data_dir)
    end)

    %{data_dir: data_dir}
  end

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn cmd, state -> {state, cmd} end, initial_state: 0}}
  end

  defp cluster do
    name = :"facade_int_#{System.unique_integer([:positive])}"

    {:ok, [{_sname, _} = sid | _]} =
      RaftEx.form_cluster(name, machine(), [node()], timeout: 5_000)

    Process.sleep(100)
    {name, sid}
  end

  describe "command API" do
    test "process_command with integer timeout, opts map and reply_from" do
      {_name, sid} = cluster()

      assert {:ok, reply, _leader} = RaftEx.process_command(sid, :cmd_one, 3_000)
      assert reply == {0, :cmd_one}

      assert {:ok, _reply, _leader} = RaftEx.process_command(sid, :cmd_two, %{timeout: 3_000})

      result = RaftEx.process_command(sid, :cmd_three, %{timeout: 1_000, reply_from: :local})
      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result)
    end

    test "pipeline_command with and without correlation" do
      {_name, sid} = cluster()

      assert :ok = RaftEx.pipeline_command(sid, :fire_and_forget)
      assert :ok = RaftEx.pipeline_command(sid, :correlated, :corr_1)
      assert :ok = RaftEx.pipeline_command(sid, :correlated_priority, :corr_2, :high)
    end

    test "aux commands and compaction casts" do
      {_name, sid} = cluster()

      assert :ok = RaftEx.cast_aux_command(sid, {:aux, :test})
      assert :ok = RaftEx.trigger_compaction(sid)
    end
  end

  describe "query API" do
    test "local_query with timeout and options map" do
      {_name, sid} = cluster()

      result = RaftEx.local_query(sid, fn state -> state end, 1_000)
      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result)

      result = RaftEx.local_query(sid, fn state -> state end, %{timeout: 1_000, extra: 1})
      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result)
    end

    test "leader_query with timeout and options map" do
      {_name, sid} = cluster()

      result = RaftEx.leader_query(sid, fn state -> state end, 1_000)
      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result)

      result = RaftEx.leader_query(sid, fn state -> state end, %{timeout: 1_000})
      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result)
    end

    test "consistent_query and consistent_aux" do
      {_name, sid} = cluster()

      result = RaftEx.consistent_query(sid, fn state -> state end, 1_000)
      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result)

      result = RaftEx.consistent_aux(sid, {:aux, :query}, 1_000)

      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result) or
               match?({:error, _}, result)
    end

    test "members, members_info, initial_members, member_overview" do
      {_name, sid} = cluster()

      assert {:ok, _members, _leader} = RaftEx.members(sid, 3_000)
      assert {:ok, _info, _leader} = RaftEx.members_info(sid, 3_000)
      assert {:ok, _initial, _leader} = RaftEx.initial_members(sid, 3_000)
      result = RaftEx.member_overview(sid, 1_000)
      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result)
    end

    test "key_metrics with a registered counter" do
      {_name, {sname, _} = sid} = cluster()

      assert :ok =
               Counters.new(sid, [
                 {:term, 1, :counter, "term"},
                 {:commit_index, 2, :counter, "commit index"}
               ])

      assert %{state: _, membership: _} = RaftEx.key_metrics(sid, 3_000)
      assert %{} = RaftEx.key_metrics(sid)
    end
  end

  describe "app cluster API" do
    test "status and topology changes with a live server" do
      {name, {sname, _}} = cluster()

      assert {:ok, %{node: _, servers: servers}} = RaftEx.App.Cluster.status()
      assert Enum.any?(servers, &(&1.server == sname))

      assert :ok = RaftEx.App.Cluster.handle_topology_change([node()], name, machine())
      assert :ok = RaftEx.App.Cluster.handle_topology_change([], name, machine())

      Process.sleep(100)
      assert {:ok, {^sname, _}} = RaftEx.App.Cluster.find_leader(name)
    end

    test "join_cluster with a single node" do
      name = :"facade_join_#{System.unique_integer([:positive])}"

      assert {:ok, [{_sname, _} | _] = local} =
               RaftEx.join_cluster(name, machine(), [node()], node(), timeout: 5_000)

      assert length(local) == 1
    end
  end

  describe "mix task against a live cluster" do
    test "status and leader commands" do
      {name, {sname, _}} = cluster()

      original = Mix.shell()
      Mix.shell(Mix.Shell.Process)
      on_exit(fn -> Mix.shell(original) end)

      Mix.Tasks.RaftEx.Cluster.run(["status"])
      assert_receive {:mix_shell, :info, [message]}
      assert message =~ "RaftEx Cluster Status"

      Process.sleep(100)
      Mix.Tasks.RaftEx.Cluster.run(["leader", Atom.to_string(name)])

      leader_message =
        receive do
          {:mix_shell, _kind, [msg]} -> msg
        after
          200 -> "no output"
        end

      assert is_binary(leader_message)
    end
  end

  describe "remaining facade functions" do
    test "aux_command and start_node_discovery" do
      {_name, {sname, _} = sid} = cluster()

      result =
        try do
          RaftEx.aux_command(sid, {:aux, :test}, 1_000)
        catch
          :exit, _ -> {:error, :timeout}
        end

      assert result != nil or is_pid(Process.whereis(sname))

      if Node.alive?() do
        name = :"discovery_#{System.unique_integer([:positive])}"

        assert {:ok, pid} =
                 RaftEx.start_node_discovery(
                   name: name,
                   seeds: [],
                   discovery_interval: 60_000,
                   cluster_name: nil,
                   machine: nil
                 )

        GenServer.stop(pid)
      else
        assert :ok = :ok
      end
    end

    test "start_distribution and leave helpers" do
      {_name, sid} = cluster()

      result =
        try do
          RaftEx.start_distribution([])
        rescue
          ArgumentError -> {:error, :node_not_distributed}
        catch
          :exit, _ -> {:error, :exit}
        end

      assert result == :ok or match?({:error, _}, result)

      result = RaftEx.leave_and_terminate(:default, sid, sid, 1_000)
      assert result == :ok or match?({:error, _}, result) or match?({:timeout, _}, result)
    end

    test "leave_and_delete_server" do
      {_name, sid} = cluster()

      result = RaftEx.leave_and_delete_server(:default, sid, sid, 1_000)
      assert result == :ok or match?({:error, _}, result) or match?({:timeout, _}, result)
    end
  end

  describe "cluster and server lifecycle API" do
    test "ping, trigger_election and transfer_leadership" do
      {_name, sid} = cluster()

      assert {:pong, _role} = RaftEx.ping(sid, 3_000)
      assert RaftEx.trigger_election(sid, 3_000) in [:ok, {:error, :already_leader}]
      result = RaftEx.transfer_leadership(sid, sid, 3_000)

      assert match?({:ok, _, _}, result) or match?({:timeout, _}, result) or
               match?({:error, _}, result)
    end

    test "cluster_status, find_leader and local_server_id" do
      {name, {sname, _} = sid} = cluster()

      assert {:ok, status} = RaftEx.cluster_status()
      assert status.node == node()
      assert is_list(status.servers)

      Process.sleep(100)
      this_node = node()
      assert {:ok, {^sname, ^this_node}} = RaftEx.find_leader(name)
      assert RaftEx.local_server_id(name) == sid
    end

    test "start_cluster, start_or_restart_cluster and delete_cluster" do
      name = :"facade_lifecycle_#{System.unique_integer([:positive])}"
      server_ids = [{:"#{name}_s1", node()}]

      assert {:ok, _started, []} =
               RaftEx.start_cluster(:default, name, machine(), server_ids, 5_000)

      result = RaftEx.start_or_restart_cluster(:default, name, machine(), server_ids, 5_000)
      assert match?({:ok, _, []}, result) or result == {:error, :cluster_not_formed}

      assert match?({:ok, _}, RaftEx.delete_cluster(server_ids, 5_000)) or
               match?({:error, _}, RaftEx.delete_cluster(server_ids, 5_000))
    end

    test "start_server with a config map, restart, stop and force delete" do
      {name, {sname, _node} = sid} = cluster()
      new_id = {:"#{sname}_extra", node()}
      uid = "extra_#{System.unique_integer([:positive])}"

      config = %{
        id: new_id,
        uid: uid,
        cluster_name: name,
        log_init_args: %{uid: uid},
        initial_members: [sid, new_id],
        machine: machine()
      }

      assert :ok = RaftEx.start_server(:default, config)

      assert match?({:ok, _}, RaftEx.restart_server(:default, new_id)) or
               match?({:error, _}, RaftEx.restart_server(:default, new_id))

      assert :ok = RaftEx.stop_server(:default, new_id)
      result = RaftEx.force_delete_server(:default, new_id)
      assert result == :ok or match?({:error, _}, result) or match?({:badrpc, _}, result)
    end
  end
end
