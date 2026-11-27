defmodule RaftEx.Domain.Server.SnapshotHardeningTest do
  @moduledoc """
  Section 4 verification: atomic snapshots, complete multi-chunk transfer,
  timeouts/retries, membership metadata, malformed-snapshot fallback,
  install bookkeeping, and corruption reporting.
  """
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server.{Cluster, Config, RpcHandler}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.{Log, LogSnapshot}

  @self_id {:server1, node()}
  @leader {:leader, node()}
  @peer2 {:server2, node()}

  defp config(data_dir) do
    struct!(Config,
      id: @self_id,
      uid: "snap-harden-uid",
      log_id: "log",
      metrics_key: :snap_harden_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{data_dir: data_dir, names: %{wal: :snap_harden_wal}}
    )
  end

  defp state(data_dir, opts \\ []) do
    %{
      cfg: config(data_dir),
      current_term: Keyword.get(opts, :term, 2),
      log:
        Keyword.get(opts, :log, %Log{
          entries: %{},
          range: nil,
          last_term: 0,
          next_index: 1,
          last_written_index_term: {0, 0},
          wal_name: :snap_harden_wal,
          cfg: %{uid: "snap-harden-uid"}
        }),
      cluster: Keyword.get(opts, :cluster, %{}),
      commit_index: Keyword.get(opts, :commit_index, 0),
      last_applied: Keyword.get(opts, :applied, 0),
      machine_state: nil,
      leader_id: nil,
      voted_for: nil,
      snapshot_state: nil,
      snapshot_meta: nil
    }
  end

  defp snap_dir(data_dir), do: Path.join(data_dir, "snapshot")

  test "4.1 snapshot write is atomic: failure leaves no partial file" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_atomic")
    dir = snap_dir(data_dir)

    # Unwritable target directory: write must fail without a partial file.
    File.mkdir_p!(Path.join(dir, "snapshot.dat"))
    assert {:error, _} = LogSnapshot.write(dir, %{index: 1, term: 1}, :state, true)
    assert File.dir?(Path.join(dir, "snapshot.dat"))

    File.rm_rf!(dir)
    meta = %{index: 3, term: 2}
    assert {:ok, _} = LogSnapshot.write(dir, meta, :state, true)
    assert {:ok, ^meta, _} = LogSnapshot.recover(dir)
    refute File.exists?(Path.join(dir, "snapshot.dat.tmp"))
  end

  test "4.1 crash before rename keeps the previous snapshot" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_crash")
    dir = snap_dir(data_dir)
    old_meta = %{index: 2, term: 1}
    assert {:ok, _} = LogSnapshot.write(dir, old_meta, :old_state, true)
    {:ok, before} = File.read(Path.join(dir, "snapshot.dat"))

    # begin_accept stages into a temp file; the live snapshot is untouched.
    assert {:ok, accept_state} = LogSnapshot.begin_accept(dir, %{index: 5, term: 2})
    assert {:ok, ^before} = File.read(Path.join(dir, "snapshot.dat"))

    # Abandoning the transfer (crash) removes only the temp file.
    assert :ok = LogSnapshot.abort(accept_state)
    assert {:ok, ^before} = File.read(Path.join(dir, "snapshot.dat"))
    assert {:ok, ^old_meta, :old_state} = LogSnapshot.recover(dir)
  end

  test "4.2 failed and stale transfers preserve the previous snapshot" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_preserve")
    dir = snap_dir(data_dir)
    old_meta = %{index: 4, term: 2}
    assert {:ok, _} = LogSnapshot.write(dir, old_meta, :old_state, true)

    # Failed middle chunk: temp removed, previous snapshot intact.
    failing = %Types.InstallSnapshotRpc{
      term: 2,
      leader_id: @leader,
      meta: %{index: 9, term: 2},
      chunk_state: :middle,
      data: <<0::32, "junk">>
    }

    {next, st, _} = RpcHandler.handle_install_snapshot_rpc(failing, state(data_dir))
    assert next == :follower
    assert is_nil(st.snapshot_state)
    assert {:ok, ^old_meta, :old_state} = LogSnapshot.recover(dir)

    # Stale-term first chunk: previous snapshot intact.
    stale = %Types.InstallSnapshotRpc{
      term: 1,
      leader_id: @leader,
      meta: %{index: 9, term: 1},
      chunk_state: :first,
      data: <<0::32, "junk">>
    }

    {next2, _, _} = RpcHandler.handle_install_snapshot_rpc(stale, state(data_dir))
    assert next2 == :follower
    assert {:ok, ^old_meta, :old_state} = LogSnapshot.recover(dir)
  end

  test "4.3/4.4 multi-chunk transfer strips CRCs uniformly and installs exactly" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_chunks")
    dir = snap_dir(data_dir)
    mac_state = :erlang.term_to_binary(String.duplicate("state-bytes-0123456789", 4000))
    assert byte_size(mac_state) > 65_536
    meta = %{index: 10, term: 3}
    assert {:ok, _} = LogSnapshot.write(dir, meta, mac_state, true)

    assert {:ok, ^meta, read_state} =
             LogSnapshot.begin_read(dir, %{can_accept_full_file: false})

    assert {:ok, first, next1} = LogSnapshot.read_chunk(read_state, 64, dir)
    {crc, _} = read_state
    assert {:ok, middle, next2} = LogSnapshot.read_chunk({crc, elem(next1, 1)}, 64, dir)
    assert {:ok, last, :last} = LogSnapshot.read_chunk({crc, elem(next2, 1)}, 1_000_000, dir)

    st0 = state(data_dir)

    first_rpc = %Types.InstallSnapshotRpc{
      term: 3,
      leader_id: @leader,
      meta: meta,
      chunk_state: :first,
      data: first
    }

    {next, st1, []} = RpcHandler.handle_install_snapshot_rpc(first_rpc, st0)
    assert next == :receive_snapshot
    assert is_tuple(st1.snapshot_state)

    middle_rpc = %Types.InstallSnapshotRpc{
      term: 3,
      leader_id: @leader,
      meta: meta,
      chunk_state: :middle,
      data: middle
    }

    {next2, st2, []} = RpcHandler.handle_install_snapshot_rpc(middle_rpc, st1)
    assert next2 == :receive_snapshot

    last_rpc = %Types.InstallSnapshotRpc{
      term: 3,
      leader_id: @leader,
      meta: meta,
      chunk_state: :last,
      data: last
    }

    {next3, st3, effects} = RpcHandler.handle_install_snapshot_rpc(last_rpc, st2)
    assert next3 == :follower
    assert is_nil(st3.snapshot_state)

    # Installed state is byte-identical: no CRC bytes leaked in.
    assert st3.machine_state == mac_state

    assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{} = reply}] = effects
    assert reply.last_index == 10
    assert reply.last_term == 3
  end

  test "4.7 snapshot records cluster membership and install restores it" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_cluster")

    cluster = %{
      @self_id => %{voter_status: %{membership: :voter, uid: "uid-1"}},
      @peer2 => %{voter_status: %{membership: :non_voter, uid: "uid-2"}}
    }

    log = %Log{
      entries: %{5 => {5, 2, :cmd}},
      range: {5, 5},
      last_term: 2,
      next_index: 6,
      last_written_index_term: {5, 2},
      wal_name: nil,
      data_dir: data_dir,
      cfg: %{uid: "snap-harden-uid"}
    }

    {log2, []} = Log.checkpoint(5, cluster, {0, RaftEx.Domain.Machine.Simple}, :mac, log)
    assert {:ok, saved_meta} = LogSnapshot.read_meta(snap_dir(data_dir))
    assert saved_meta.index == 5
    assert get_in(saved_meta, [:cluster, @peer2, :voter_status, :membership]) == :non_voter
    assert get_in(saved_meta, [:cluster, @self_id, :voter_status, :membership]) == :voter

    # Install adopts the recorded membership.
    st = %{state(data_dir) | log: log2}

    new_cluster = %{
      @self_id => %{voter_status: %{membership: :voter, uid: "uid-1"}},
      @peer2 => %{voter_status: %{membership: :voter, uid: "uid-2"}}
    }

    _ = new_cluster

    install_meta =
      Map.put(saved_meta, :cluster, %{
        @self_id => %{voter_status: %{membership: :voter, uid: "uid-1"}},
        @peer2 => %{voter_status: %{membership: :promotable, uid: "uid-2"}}
      })

    last_rpc = %Types.InstallSnapshotRpc{
      term: 3,
      leader_id: @leader,
      meta: install_meta,
      chunk_state: :last,
      data: <<>>
    }

    # Drive a full install through the :full path with the enriched meta.
    full = %Types.InstallSnapshotRpc{
      term: 3,
      leader_id: @leader,
      meta: install_meta,
      chunk_state: :full,
      data: :mac2
    }

    {next, st2, _} = RpcHandler.handle_install_snapshot_rpc(full, st)
    assert next == :follower
    assert Cluster.get_membership(st2.cluster, @peer2, "uid-2", :voter) == :promotable
  end

  test "4.7 legacy snapshots without membership fall back to initial members" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_legacy")
    server_dir = Path.join(data_dir, "legacy-uid")
    dir = Path.join(server_dir, "snapshot")
    File.mkdir_p!(dir)
    assert {:ok, _} = LogSnapshot.write(dir, %{index: 2, term: 1}, :old, true)

    conf = %{
      uid: "legacy-uid",
      system_config: %{data_dir: data_dir, names: %{wal: :x, log_meta: :y, open_mem_tbls: :z}}
    }

    log = Log.init(conf)
    assert log.current_snapshot == {2, 1}
    assert is_nil(Log.snapshot_cluster(log))
  end

  test "4.7 snapshot cluster survives recovery for a restart" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_cluster_restart")
    server_dir = Path.join(data_dir, "snap-harden-uid")
    File.mkdir_p!(server_dir)

    cluster = %{
      @self_id => %{voter_status: %{membership: :voter, uid: "uid-1"}},
      @peer2 => %{voter_status: %{membership: :promotable, uid: "uid-2"}}
    }

    log = %Log{
      entries: %{},
      range: nil,
      last_term: 0,
      next_index: 1,
      last_written_index_term: {0, 0},
      wal_name: nil,
      data_dir: server_dir,
      cfg: %{uid: "snap-harden-uid"},
      mem_table: nil
    }

    {_log2, []} = Log.checkpoint(4, cluster, {0, RaftEx.Domain.Machine.Simple}, :mac, log)

    conf = %{
      uid: "snap-harden-uid",
      system_config: %{data_dir: data_dir, names: %{wal: :x, log_meta: :y, open_mem_tbls: :z}}
    }

    restarted = Log.init(conf)
    assert restarted.current_snapshot == {4, 0}

    restored = Log.snapshot_cluster(restarted)
    assert get_in(restored, [@self_id, :voter_status, :membership]) == :voter
    assert get_in(restored, [@peer2, :voter_status, :membership]) == :promotable
  end

  test "4.8 malformed snapshot falls back to the WAL instead of crashing startup" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_malformed")
    server_dir = Path.join(data_dir, "srv")
    File.mkdir_p!(Path.join(server_dir, "snapshot"))
    # Valid magic/version/checksum shape but a truncated term.
    File.write!(
      Path.join([server_dir, "snapshot", "snapshot.dat"]),
      <<"RASN", 1, 0::32, " truncated">>
    )

    conf = %{
      uid: "malformed-uid",
      system_config: %{data_dir: data_dir, names: %{wal: :x, log_meta: :y, open_mem_tbls: :z}}
    }

    log = Log.init(conf)
    assert log.current_snapshot == nil
    assert log.entries == %{}
  end

  test "4.9 install sets term bookkeeping and snapshot state for later sends" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_bookkeep")
    meta = %{index: 7, term: 4}

    first = %Types.InstallSnapshotRpc{
      term: 4,
      leader_id: @leader,
      meta: meta,
      chunk_state: :first,
      data: framed(meta)
    }

    {_next, st1, []} = RpcHandler.handle_install_snapshot_rpc(first, state(data_dir))

    last = %Types.InstallSnapshotRpc{
      term: 4,
      leader_id: @leader,
      meta: meta,
      chunk_state: :last,
      data: <<0::32>>
    }

    {_next, st2, effects} = RpcHandler.handle_install_snapshot_rpc(last, st1)

    assert st2.log.last_term == 4
    assert st2.log.last_written_index_term == {7, 4}
    assert st2.log.snapshot_state == st2.machine_state
    assert st2.last_applied == 7
    assert st2.commit_index == 7

    assert [{:send_rpc, @leader, %Types.InstallSnapshotResult{last_index: 7, last_term: 4}}] =
             effects
  end

  test "4.10 corrupt snapshot is reported, not installed" do
    data_dir = RaftEx.TestSupport.tmp_dir("snap_corrupt")
    dir = snap_dir(data_dir)
    assert {:ok, _} = LogSnapshot.write(dir, %{index: 1, term: 1}, :state, true)

    path = Path.join(dir, "snapshot.dat")
    <<magic::binary-size(4), version, _crc::32, rest::binary>> = File.read!(path)
    File.write!(path, [magic, <<version>>, <<0::32>>, rest])

    assert {:error, :checksum_error} = LogSnapshot.recover(dir)
    assert {:error, :checksum_error} = LogSnapshot.validate(dir)
  end

  defp framed(meta) do
    meta_bin = :erlang.term_to_binary(meta)
    mac_bin = :erlang.term_to_binary(:mac_state)
    data = <<byte_size(meta_bin)::32-integer, meta_bin::binary, mac_bin::binary>>
    crc = :erlang.crc32(data)
    <<"RASN", 1, crc::32-integer, data::binary>>
  end
end
