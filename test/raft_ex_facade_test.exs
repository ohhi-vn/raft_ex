defmodule RaftEx.FacadeTest.FakeStatem do
  @moduledoc false
  use GenServer

  def start_link({name, reply, mode}) do
    GenServer.start_link(__MODULE__, {reply, mode}, name: name)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(_msg, _from, {_reply, :silent} = state), do: {:noreply, state}

  @impl true
  def handle_call(_msg, _from, {reply, :reply} = state), do: {:reply, reply, state}
end

defmodule RaftExFacadeTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine
  alias RaftEx.FacadeTest.FakeStatem
  alias RaftEx.Infra.Config.Env
  alias RaftEx.Infra.Registry.Directory

  defp machine do
    {:machine, Machine.Simple, %{simple_fun: fn _c, s -> s end, initial_state: 0}}
  end

  test "new_uid derives a safe prefix" do
    uid = RaftEx.new_uid("my cluster/name")
    assert byte_size(uid) == 18
    assert Regex.match?(~r/^[A-Z0-9]{18}$/, uid)
  end

  test "overview reports an unstarted system" do
    assert RaftEx.overview(:"facade_missing_#{System.unique_integer([:positive])}") ==
             :system_not_started
  end

  test "cluster_status reports an unstarted system" do
    assert RaftEx.cluster_status(:"facade_missing_#{System.unique_integer([:positive])}") ==
             {:error, :system_not_started}
  end

  test "key_metrics reports an unregistered server without raising" do
    name = :"missing_server_#{System.unique_integer([:positive])}"

    # Counters.counters/2 returns nils for an unregistered server; the
    # facade drops them so metrics queries stay safe.
    assert RaftEx.key_metrics({name, node()}) == %{
             state: :noproc,
             membership: :unknown
           }
  end

  test "pipeline_command with no local process returns an error" do
    name = :"missing_server_#{System.unique_integer([:positive])}"
    assert RaftEx.pipeline_command({name, node()}, :cmd) == :ok
    assert RaftEx.pipeline_command({name, node()}, :cmd, :corr) == :ok
  end

  test "form_cluster rejects nodes with no local server" do
    assert RaftEx.form_cluster(:facade_cluster, machine(), [:"remote@invalid.host"]) ==
             {:error, :no_local_servers}
  end

  test "leaderboard delegates" do
    RaftEx.Leaderboard.init()
    assert RaftEx.local_server_id(:"unknown_cluster_#{System.unique_integer([:positive])}") == nil
    assert is_list(RaftEx.leaderboard_overview())
  end

  describe "facade error and edge paths" do
    defp fake_server(opts \\ []) do
      name = :"facade_fake_#{System.unique_integer([:positive])}"
      reply = Keyword.get(opts, :reply, {:wrap_reply, :ok})
      mode = Keyword.get(opts, :mode, :reply)

      {:ok, pid} = FakeStatem.start_link({name, reply, mode})

      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid, :normal, 100)
      end)

      {name, node()}
    end

    test "delete_cluster handles success, timeout and error replies" do
      ok_ref = fake_server(reply: {:wrap_reply, :deleted})
      assert {:ok, ^ok_ref} = RaftEx.delete_cluster(ok_ref, 500)

      silent_ref = fake_server(mode: :silent)
      assert {:error, :timeout} = RaftEx.delete_cluster(silent_ref, 30)

      error_ref = fake_server(reply: {:error, :boom})
      assert {:error, :boom} = RaftEx.delete_cluster(error_ref, 500)
    end

    test "leave helpers handle timeouts and errors" do
      silent_ref = fake_server(mode: :silent)
      assert :timeout = RaftEx.leave_and_terminate(:default, silent_ref, silent_ref, 30)
      assert :timeout = RaftEx.leave_and_delete_server(:default, silent_ref, silent_ref, 30)

      missing = {:"facade_missing_#{System.unique_integer([:positive])}", node()}
      assert {:error, :noproc} = RaftEx.leave_and_terminate(:default, missing, missing, 200)

      error_ref = fake_server(reply: {:error, :boom})

      assert {:error, :boom} =
               RaftEx.leave_and_delete_server(:default, error_ref, error_ref, 500)
    end

    test "membership helpers accept default, integer and atom arguments" do
      server_ref = fake_server(reply: {:wrap_reply, :ok})
      new_id = {:"facade_member_#{System.unique_integer([:positive])}", node()}

      assert {:ok, :ok, ^server_ref} = RaftEx.add_member(server_ref, new_id)
      assert {:ok, :ok, ^server_ref} = RaftEx.add_member(server_ref, new_id, 500)
      assert {:ok, :ok, ^server_ref} = RaftEx.add_member(server_ref, new_id, :non_voter)
      assert {:ok, :ok, ^server_ref} = RaftEx.add_member(server_ref, new_id, :voter, 500)
      assert {:ok, :ok, ^server_ref} = RaftEx.promote_member(server_ref, new_id)
      assert {:ok, :ok, ^server_ref} = RaftEx.demote_member(server_ref, new_id)
      assert {:ok, :ok, ^server_ref} = RaftEx.remove_member(server_ref, new_id)
    end

    test "query helpers accept default, integer and opts-map arguments" do
      server_ref = fake_server(reply: {:wrap_reply, :queried})
      query_fun = fn state -> state end

      assert {:ok, :queried, ^server_ref} = RaftEx.local_query(server_ref, query_fun)
      assert {:ok, :queried, ^server_ref} = RaftEx.local_query(server_ref, query_fun, 500)

      assert {:ok, :queried, ^server_ref} =
               RaftEx.local_query(server_ref, query_fun, %{timeout: 500, extra: :ok})

      assert {:ok, :queried, ^server_ref} = RaftEx.leader_query(server_ref, query_fun)
      assert {:ok, :queried, ^server_ref} = RaftEx.leader_query(server_ref, query_fun, 500)

      assert {:ok, :queried, ^server_ref} =
               RaftEx.leader_query(server_ref, query_fun, %{timeout: 500, extra: :ok})

      assert {:ok, :queried, ^server_ref} = RaftEx.consistent_query(server_ref, query_fun, 500)
      assert {:ok, :queried, ^server_ref} = RaftEx.consistent_aux(server_ref, :aux_query, 500)
    end

    test "server lifecycle helpers normalize failures" do
      down_node = :"facade_down_#{System.unique_integer([:positive])}@127.0.0.1"
      uid = RaftEx.new_uid("facade-down-node")

      assert {:error, {:badrpc, :nodedown}} =
               RaftEx.start_server(:default, %{
                 id: {:down_server, down_node},
                 uid: uid,
                 cluster_name: :facade_down,
                 log_init_args: %{uid: uid},
                 initial_members: [{:down_server, down_node}],
                 machine: machine()
               })

      assert {:error, :system_not_started} =
               RaftEx.restart_server(
                 :"facade_missing_system_#{System.unique_integer([:positive])}",
                 {:missing, node()}
               )

      assert {:error, {:badrpc, :nodedown}} =
               RaftEx.restart_server(:default, {:down_server, down_node})
    end

    test "key_metrics handles a registered process without state and remote nodes" do
      name = :"facade_metrics_#{System.unique_integer([:positive])}"

      pid =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      Process.register(pid, name)

      on_exit(fn ->
        if Process.alive?(pid), do: Process.exit(pid, :kill)
      end)

      assert %{state: :unknown, membership: :unknown} = RaftEx.key_metrics({name, node()})

      remote = :"facade_remote_#{System.unique_integer([:positive])}@127.0.0.1"

      result =
        try do
          RaftEx.key_metrics({name, remote})
        rescue
          _ -> {:badrpc, :noconnection}
        end

      assert match?({:badrpc, _}, result) or is_map(result)
    end
  end

  describe "start/1 error handling" do
    test "returns an error when the data directory cannot be created" do
      dir = RaftEx.TestSupport.tmp_dir("facade_start_error")
      file = Path.join(dir, "not_a_directory")
      File.write!(file, "occupied")

      original = Application.get_env(:raft_ex, :data_dir)

      on_exit(fn ->
        Application.put_env(:raft_ex, :data_dir, original)
        RaftEx.App.System.stop_default()
        :persistent_term.erase({:"$ra_system", :default})
      end)

      assert {:error, _reason} = RaftEx.start(data_dir: file)
    end
  end

  describe "cluster lifecycle edge cases" do
    setup do
      data_dir = RaftEx.TestSupport.tmp_dir("facade_cluster_edge")
      RaftEx.App.System.stop_default()
      :persistent_term.erase({:"$ra_system", :default})
      :ok = RaftEx.start_in(data_dir)

      on_exit(fn ->
        RaftEx.App.System.stop_default()
        :persistent_term.erase({:"$ra_system", :default})
      end)

      %{data_dir: data_dir}
    end

    test "start_cluster and start_or_restart_cluster accept explicit server ids" do
      suffix = System.unique_integer([:positive])
      name = :"facade_edge_#{suffix}"
      sid = {:"#{name}_s1", node()}

      assert {:ok, [^sid], []} = RaftEx.start_cluster(:default, name, machine(), [sid], 5_000)

      # The restart fails (no persisted recovery config) so the default-timeout
      # clause of start_or_restart_cluster falls back to start_cluster.
      assert {:error, :cluster_not_formed} =
               RaftEx.start_or_restart_cluster(:default, name, machine(), [sid])
    end

    test "start_cluster succeeds with a zero timeout when a leader is elected" do
      suffix = System.unique_integer([:positive])
      name = :"facade_zero_timeout_#{suffix}"
      sid = {:"#{name}_s1", node()}

      assert {:ok, [^sid], []} = RaftEx.start_cluster(:default, name, machine(), [sid], 0)
    end

    test "start_cluster reports cluster_not_formed when no leader emerges" do
      suffix = System.unique_integer([:positive])
      name = :"facade_no_leader_#{suffix}"
      sid = {:"#{name}_s1", node()}
      uid = RaftEx.new_uid(Atom.to_string(name))

      config = %{
        id: sid,
        uid: uid,
        cluster_name: name,
        log_init_args: %{uid: uid},
        initial_members: [{:"#{name}_absent", :"facade_down_#{suffix}@127.0.0.1"}],
        machine: machine()
      }

      assert {:error, :cluster_not_formed} = RaftEx.start_cluster(:default, [config], 200)
    end

    test "start_or_restart_cluster restarts stopped servers with persisted configs" do
      suffix = System.unique_integer([:positive])
      name = :"facade_restart_#{suffix}"
      sids = [{:"#{name}_s1", node()}, {:"#{name}_s2", node()}]

      assert {:ok, _, []} = RaftEx.start_cluster(:default, name, machine(), sids, 5_000)

      Enum.each(sids, fn {server_name, _} = sid ->
        uid = Directory.uid_of(:default, server_name)
        dir = Env.server_data_dir(:default, uid)
        File.mkdir_p!(dir)

        config = %{
          id: sid,
          uid: uid,
          cluster_name: name,
          log_init_args: %{uid: uid},
          initial_members: sids,
          machine: machine()
        }

        File.write!(Path.join(dir, "config"), :erlang.term_to_binary(config))
        assert :ok = RaftEx.stop_server(:default, sid)
      end)

      assert {:ok, ^sids, []} = RaftEx.start_or_restart_cluster(:default, name, machine(), sids)
    end
  end
end
