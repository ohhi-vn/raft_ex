defmodule RaftEx.MultiNodeTest do
  use ExUnit.Case, async: false

  @test_cookie :raft_ex_test_cookie

  setup do
    data_dir = Path.join(System.tmp_dir!(), "raft_ex_multi_#{System.unique_integer()}")
    File.mkdir_p!(data_dir)

    RaftEx.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    RaftEx.start_in(data_dir)

    on_exit(fn ->
      File.rm_rf!(data_dir)
      for [name] <- :ets.match(:ra_state, {:"$1", :_, :_}) do
        case Process.whereis(name) do
          nil -> :ok
          pid -> :gen_statem.stop(pid)
        end
      end
      :ets.match_delete(:ra_state, {:_, :_, :_})
    end)

    %{data_dir: data_dir}
  end

  describe "node distribution" do
    test "build_server_ids/2 creates correct server id tuples" do
      nodes = [:"node1@host", :"node2@host", :"node3@host"]
      server_ids = RaftEx.Distribution.build_server_ids(nodes)

      assert length(server_ids) == 3
      assert {:server1, :"node1@host"} in server_ids
      assert {:server2, :"node2@host"} in server_ids
      assert {:server3, :"node3@host"} in server_ids
    end

    test "connect_to_seeds/2 handles empty seed list" do
      connected = RaftEx.Distribution.connect_to_seeds([], @test_cookie)
      assert connected == []
    end

    test "cluster_nodes/0 includes self" do
      nodes = RaftEx.Distribution.cluster_nodes()
      assert node() in nodes
    end

    test "build_server_ids/2 uses custom prefix" do
      nodes = [:"n1@host", :"n2@host"]
      ids = RaftEx.Distribution.build_server_ids(nodes, :raft)
      assert {:raft1, :"n1@host"} in ids
      assert {:raft2, :"n2@host"} in ids
    end
  end

  describe "leaderboard" do
    test "record and lookup leader" do
      leader = {:server1, node()}
      members = [{:server1, node()}, {:server2, node()}]

      :ok = RaftEx.Leaderboard.record(:test_cluster, leader, members)

      assert RaftEx.Leaderboard.lookup_leader(:test_cluster) == leader
      assert RaftEx.Leaderboard.lookup_members(:test_cluster) == members
      assert RaftEx.Leaderboard.lookup_timestamp(:test_cluster) != nil

      RaftEx.Leaderboard.clear(:test_cluster)
    end

    test "clear cluster" do
      RaftEx.Leaderboard.record(:temp_cluster, {:s1, node()}, [{:s1, node()}])
      RaftEx.Leaderboard.clear(:temp_cluster)

      assert RaftEx.Leaderboard.lookup_leader(:temp_cluster) == nil
    end

    test "local_server_id returns self when in members" do
      RaftEx.Leaderboard.record(
        :cluster_a,
        {:s1, node()},
        [{:s1, node()}, {:s2, :"other@host"}]
      )

      assert RaftEx.Leaderboard.local_server_id(:cluster_a) == {:s1, node()}
      RaftEx.Leaderboard.clear(:cluster_a)
    end

    test "local_server_id returns nil when node not in cluster" do
      RaftEx.Leaderboard.record(
        :cluster_b,
        {:s1, :"other@host"},
        [{:s1, :"other@host"}]
      )

      assert RaftEx.Leaderboard.local_server_id(:cluster_b) == nil
      RaftEx.Leaderboard.clear(:cluster_b)
    end

    test "overview returns formatted entries" do
      RaftEx.Leaderboard.record(:cluster_c, {:s1, node()}, [{:s1, node()}])

      overview = RaftEx.Leaderboard.overview()
      assert length(overview) > 0
      entry = Enum.find(overview, fn e -> e.cluster == :cluster_c end)
      assert entry != nil
      assert entry.leader == {:s1, node()}
      assert is_struct(entry.recorded_at, DateTime)

      RaftEx.Leaderboard.clear(:cluster_c)
    end
  end

  describe "cluster API" do
    test "form_cluster with single node starts servers", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_single_#{System.unique_integer([:positive])}"
      result = RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      assert {:ok, [{sname, _} | _]} = result

      server_id = {sname, node()}
      {:ok, members, _leader} = RaftEx.members(server_id, 2_000)
      assert sname in Enum.map(members, fn {n, _} -> n end)
    end

    test "process_command works after forming cluster", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_cmd_#{System.unique_integer([:positive])}"

      {:ok, [{sname, _} = sid | _]} =
        RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      {:ok, reply, _leader} = RaftEx.process_command(sid, :hello)
      assert reply == {0, :hello}
    end

    test "cluster_status returns server info", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_status_#{System.unique_integer([:positive])}"
      {:ok, [_sid | _]} =
        RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      {:ok, status} = RaftEx.cluster_status()
      assert is_list(status.servers)
      assert length(status.servers) > 0
      assert status.node == node()
    end

    test "find_leader returns leader via leaderboard", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_leader_find_#{System.unique_integer([:positive])}"
      {:ok, [{sname, _} | _]} =
        RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      Process.sleep(200)

      {:ok, leader} = RaftEx.find_leader(cluster_name)
      assert elem(leader, 0) == sname
    end

    test "members retrieved after cluster formation", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_members_#{System.unique_integer([:positive])}"

      {:ok, [{sname, _} = sid | _]} =
        RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      {:ok, members, _leader} = RaftEx.members(sid, 2_000)
      assert length(members) > 0
    end

    test "leaderboard_overview returns formatted data", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_lb_#{System.unique_integer([:positive])}"
      {:ok, _} =
        RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      Process.sleep(200)

      overview = RaftEx.leaderboard_overview()
      entry = Enum.find(overview, fn e -> e.cluster == cluster_name end)
      assert entry != nil, "Leaderboard should have entry for #{cluster_name}"
      assert entry.leader != nil
    end
  end

  describe "cluster membership changes" do
    test "add_member works on single node", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_add_#{System.unique_integer([:positive])}"

      {:ok, [{sname, _} = sid | _]} =
        RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      new_name = :"#{sname}_new"
      new_id = {new_name, node()}

      RaftEx.start_server(:default, cluster_name, new_id, machine, [sid, new_id])
      :timer.sleep(100)

      result = RaftEx.add_member(sid, new_id, 5_000)
      assert {:ok, _, _} = result
    end

    test "remove_member works on single node", %{data_dir: _dd} do
      machine =
        {:machine, RaftEx.MachineSimple,
         %{simple_fun: fn cmd, s -> {s, cmd} end, initial_state: 0}}

      cluster_name = :"test_remove_#{System.unique_integer([:positive])}"

      {:ok, [{sname, _} = sid | _]} =
        RaftEx.form_cluster(cluster_name, machine, [node()], timeout: 5_000)

      new_name = :"#{sname}_rm"
      new_id = {new_name, node()}

      RaftEx.start_server(:default, cluster_name, new_id, machine, [sid, new_id])
      :timer.sleep(100)
      RaftEx.add_member(sid, new_id, 5_000)
      :timer.sleep(100)

      result = RaftEx.remove_member(sid, new_id, 5_000)
      assert {:ok, _, _} = result

      {:ok, members, _} = RaftEx.members(sid, 2_000)
      refute new_id in members
    end
  end
end
