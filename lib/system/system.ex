defmodule RaftEx.System do
  require Logger

  @type names :: %{
          wal: atom(),
          wal_sup: atom(),
          log_sup: atom(),
          log_ets: atom(),
          log_meta: atom(),
          log_sync: atom(),
          open_mem_tbls: atom(),
          segment_writer: atom(),
          server_sup: atom(),
          directory: atom(),
          directory_rev: atom()
        }

  @type config :: map()

  def start(%{name: name} = config) do
    Logger.info("ra: starting system #{name}")
    RaftEx.SystemsSupervisor.start_system(config)
  end

  def start_default, do: start(default_config())

  def default_config do
    data_dir = RaftEx.Env.data_dir()

    %{
      name: :default,
      data_dir: data_dir,
      wal_data_dir: data_dir,
      wal_max_size_bytes: 256_000_000,
      wal_compute_checksums: true,
      wal_max_batch_size: 8192,
      wal_max_entries: nil,
      wal_sync_method: :datasync,
      segment_max_entries: 4096,
      segment_max_size_bytes: 64_000_000,
      segment_compute_checksums: true,
      default_max_pipeline_count: 4096,
      default_max_append_entries_rpc_batch_size: 128,
      machine_upgrade_strategy: :all,
      broadcast_time: 100,
      election_timeout_min: 500,
      election_timeout_max: 2000,
      heartbeat_interval: 100,
      names: default_names()
    }
  end

  def default_names do
    %{
      wal: :ra_log_wal,
      wal_sup: :ra_log_wal_sup,
      log_sup: :ra_log_sup,
      log_ets: :ra_log_ets,
      log_meta: :ra_log_meta,
      log_sync: :ra_log_sync,
      open_mem_tbls: :ra_log_open_mem_tables,
      segment_writer: :ra_log_segment_writer,
      server_sup: :ra_server_sup_sup,
      directory: :ra_directory,
      directory_rev: :ra_directory_reverse
    }
  end

  def derive_names(sys_name) when is_atom(sys_name) do
    s = Atom.to_string(sys_name)
    derive = fn suf -> String.to_atom("ra_#{s}_#{suf}") end

    %{
      wal: derive.("log_wal"),
      wal_sup: derive.("log_wal_sup"),
      log_sup: derive.("log_sup"),
      log_ets: derive.("log_ets"),
      log_meta: derive.("log_meta"),
      log_sync: derive.("log_sync"),
      open_mem_tbls: derive.("log_open_mem_tables"),
      segment_writer: derive.("segment_writer"),
      server_sup: derive.("server_sup_sup"),
      directory: derive.("directory"),
      directory_rev: derive.("directory_reverse")
    }
  end

  def store(%{name: name} = config) do
    :persistent_term.put({:"$ra_system", name}, config)
  end

  def fetch(name) when is_atom(name) do
    :persistent_term.get({:"$ra_system", name}, nil)
  end

  def fetch(name, node) when is_atom(name) and is_atom(node) do
    :rpc.call(node, :persistent_term, :get, [{:"$ra_system", name}, nil])
  end

  def lookup_name(system, key) when is_atom(system) do
    case fetch(system) do
      nil -> {:error, :system_not_started}
      %{names: names} -> {:ok, Map.fetch!(names, key)}
    end
  end

  def stop(system) do
    RaftEx.SystemsSupervisor.stop_system(system)
  end

  def stop_default do
    %{name: name} = default_config()
    stop(name)
  end
end
