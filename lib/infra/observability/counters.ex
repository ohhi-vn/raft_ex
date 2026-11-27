defmodule RaftEx.Infra.Observability.Counters do
  @moduledoc """
  Counter helpers backed by the ETS-backed `RaftEx.Infra.Observability.Metrics`
  store.

  This module preserves the historical API surface (init/new/fetch/delete/
  overview/counters and the inc/observe/set_gauge helpers) but no longer depends
  on `:seshat`; all data lives in the ETS-backed `Metrics` store.
  """

  alias RaftEx.Infra.Observability.{MetricNames, Metrics}

  # ---------------------------------------------------------------------------
  # Initialisation
  # ---------------------------------------------------------------------------

  @doc "Initialise the metrics store. Safe to call more than once."
  @spec init() :: :ok
  def init do
    Metrics.init()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Per-server counter management (no-op registration; ETS auto-creates keys)
  # ---------------------------------------------------------------------------

  @spec new(term(), list()) :: :ok
  def new(_name, _fields_spec) do
    :ok
  end

  @spec new(term(), list(), map() | term()) :: :ok
  def new(_name, _fields_spec, _labels) do
    :ok
  end

  @spec fetch(term()) :: nil
  def fetch(_name) do
    nil
  end

  @spec delete(term()) :: :ok
  def delete(_name) do
    :ok
  end

  # ---------------------------------------------------------------------------
  # Overview / metrics
  # ---------------------------------------------------------------------------

  @spec overview() :: map()
  def overview do
    Metrics.all()
  end

  @spec overview(term()) :: map()
  def overview(_name) do
    Metrics.all()
  end

  @doc """
  Read per-server metric values for `fields` from the metrics store.
  Counter, gauge, and histogram values written via `inc/3`, `set_gauge/3`,
  and `observe/3` are returned keyed by field name; missing fields read
  as `nil`.
  """
  @spec counters(term(), [atom()]) :: map()
  def counters(server_id, fields) when is_list(fields) do
    Map.new(fields, fn field -> {field, Metrics.get({server_id, field})} end)
  end

  def counters(_server_id, _fields), do: %{}

  # ---------------------------------------------------------------------------
  # Counter fields
  # ---------------------------------------------------------------------------

  @spec counter_fields() :: [tuple()]
  def counter_fields do
    MetricNames.all()
    |> Enum.filter(fn {_name, kind} -> kind == :counter end)
    |> Enum.map(fn {name, :counter} -> name end)
    |> Enum.with_index(1)
    |> Enum.map(fn {name, idx} -> {name, idx, :counter, Atom.to_string(name)} end)
  end

  # ---------------------------------------------------------------------------
  # Convenience (delegates to ETS-backed Metrics)
  # ---------------------------------------------------------------------------

  @spec inc(term(), number()) :: :ok
  def inc(name, amount \\ 1) do
    Metrics.inc(name, amount)
  end

  @spec observe(term(), number()) :: :ok
  def observe(name, value) do
    Metrics.observe(name, value)
  end

  @spec set_gauge(term(), number()) :: :ok
  def set_gauge(name, value) do
    Metrics.set(name, value)
  end

  # Per-server helpers (keys are namespaced by server id in Metrics)
  @spec inc(term(), term(), number()) :: :ok
  def inc(server_id, name, amount) do
    Metrics.inc({server_id, name}, amount)
  end

  @spec observe(term(), term(), number()) :: :ok
  def observe(server_id, name, value) do
    Metrics.observe({server_id, name}, value)
  end

  @spec set_gauge(term(), term(), number()) :: :ok
  def set_gauge(server_id, name, value) do
    Metrics.set({server_id, name}, value)
  end
end
