defmodule RaftEx.Observability do
  @moduledoc """
  Read API for RaftEx runtime metrics.

  Read API for the ETS-backed `RaftEx.Infra.Observability.Metrics` store.

  See the [Observability guide](guides/observability.md).
  """

  alias RaftEx.Infra.Observability.{Counters, Metrics}

  @doc "Return all ETS-backed metrics as a map."
  @spec metrics() :: map()
  def metrics, do: Metrics.all()

  @doc "Read a single ETS metric by name."
  @spec metric(term()) :: term()
  def metric(name), do: Metrics.get(name)

  @doc "Return the metrics overview."
  @spec seshat_overview() :: map()
  def seshat_overview, do: Counters.overview()

  @doc "Return per-server counters (empty map; ETS is flat)."
  @spec server_metrics(term()) :: map()
  def server_metrics(_server_id) do
    %{}
  end

  @doc "Return cluster-wide seshat overview."
  @spec cluster_metrics(term()) :: map()
  def cluster_metrics(_cluster \\ nil) do
    Counters.overview()
  end

  @doc "Reset all ETS-backed metrics."
  @spec reset() :: :ok
  def reset, do: Metrics.reset()
end
