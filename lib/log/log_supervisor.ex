defmodule RaftEx.LogSupervisor do
  use Supervisor

  def start_link(%{names: %{log_sup: name}} = cfg) do
    Supervisor.start_link(__MODULE__, cfg, name: name)
  end

  @impl Supervisor
  def init(%{data_dir: data_dir, names: %{wal: _wal, segment_writer: seg_writer_name}} = cfg) do
    pre_init = %{id: :ra_log_pre_init, start: {RaftEx.LogPreInit, :start_link, [cfg.name]}}

    meta = %{id: :ra_log_meta, start: {RaftEx.LogMeta, :start_link, [cfg]}}

    seg_writer_conf = %{
      name: seg_writer_name,
      system: cfg.name,
      data_dir: data_dir,
      segment_conf: %{
        max_count: Map.get(cfg, :segment_max_entries, 4096),
        max_size: Map.get(cfg, :segment_max_size_bytes, 64_000_000),
        compute_checksums: Map.get(cfg, :segment_compute_checksums, true)
      }
    }

    seg_writer = %{
      id: :ra_log_segment_writer,
      start: {RaftEx.LogSegmentWriter, :start_link, [seg_writer_conf]},
      shutdown: 30_000
    }

    wal_conf = make_wal_conf(cfg)

    wal_sup = %{
      id: :ra_log_wal_sup,
      type: :supervisor,
      start: {RaftEx.LogWalSupervisor, :start_link, [wal_conf]}
    }

    children = [pre_init, meta, seg_writer, wal_sup]

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 5, max_seconds: 5)
  end

  defp make_wal_conf(%{data_dir: data_dir, names: names} = cfg) do
    %{
      names: names,
      system: cfg.name,
      dir: Map.get(cfg, :wal_data_dir, data_dir),
      compute_checksums: Map.get(cfg, :wal_compute_checksums, true),
      max_size_bytes: Map.get(cfg, :wal_max_size_bytes, 256_000_000),
      max_entries: Map.get(cfg, :wal_max_entries),
      sync_method: Map.get(cfg, :wal_sync_method, :datasync),
      max_batch_size: Map.get(cfg, :wal_max_batch_size, 8192)
    }
  end
end
