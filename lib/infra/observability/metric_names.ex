defmodule RaftEx.Infra.Observability.MetricNames do
  @moduledoc """
  Catalogue of all RaftEx metric names and their kinds.

  Single source of truth for metric atoms used by both the ETS-backed
  `RaftEx.Infra.Observability.Metrics` store and the compatibility
  `RaftEx.Infra.Observability.Counters` layer.
  """

  @metrics [
    # WAL
    {:wal_flushes, :counter},
    {:wal_flush_errors, :counter},
    {:wal_rotation_errors, :counter},
    {:wal_flush_latency_ms, :histogram},
    {:wal_bytes_written, :counter},
    {:wal_segment_writes, :counter},
    {:wal_fsyncs, :counter},
    # Election
    {:elections_started, :counter},
    {:elections_won, :counter},
    {:elections_lost, :counter},
    {:term_changed, :counter},
    {:votes_granted, :counter},
    # Quorum / commit
    {:quorum_reached, :counter},
    {:commit_advanced, :counter},
    {:commit_index, :gauge},
    # Replication
    {:replication_backpressure, :counter},
    {:append_entries_sent, :counter},
    {:append_entries_acked, :counter},
    {:replication_lag, :gauge},
    # Snapshots
    {:snapshot_chunks_sent, :counter},
    {:snapshot_chunks_received, :counter},
    {:snapshot_chunks_installed, :counter},
    {:snapshot_chunks_failed, :counter},
    {:snapshot_transfer_duration_ms, :histogram},
    # Leadership transfer
    {:leadership_transfers_initiated, :counter},
    {:leadership_transfers_completed, :counter},
    {:leadership_transfers_failed, :counter}
  ]

  @doc "Return all `{name, kind}` pairs."
  @spec all() :: [{atom(), atom()}]
  def all, do: @metrics

  @doc "Return all metric names."
  @spec names() :: [atom()]
  def names, do: Enum.map(@metrics, &elem(&1, 0))

  @doc "Return the kind for a metric name, or nil."
  @spec kind(atom()) :: atom() | nil
  def kind(name) do
    case List.keyfind(@metrics, name, 0) do
      {^name, kind} -> kind
      nil -> nil
    end
  end
end
