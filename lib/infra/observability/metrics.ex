defmodule RaftEx.Infra.Observability.Metrics do
  @moduledoc """
  Lightweight, ETS-backed metrics for RaftEx.

  Primary metrics store, backed by ETS. Values are kept in the
  `:raft_ex_metrics` ETS table (created by `MetricsEts`).

  Supported metric kinds:
    * `:counter`  - monotonically increasing integer
    * `:gauge`    - settable integer/float
    * `:histogram`- accumulated sum + count (min/max tracked)
  """

  @table :raft_ex_metrics

  @doc "Ensure the backing table exists (idempotent)."
  @spec init() :: :ok
  def init do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :named_table,
          :public,
          :set,
          {:write_concurrency, true},
          {:read_concurrency, true}
        ])

        :ok

      _ ->
        :ok
    end
  end

  @doc "Increment a counter by `amount` (default 1)."
  @spec inc(atom() | String.t(), number()) :: :ok
  def inc(name, amount \\ 1) do
    init()
    key = {:counter, name}

    case :ets.lookup(@table, key) do
      [{^key, value}] -> :ets.insert(@table, {key, value + amount})
      [] -> :ets.insert(@table, {key, amount})
    end

    :ok
  end

  @doc "Set a gauge value."
  @spec set(atom() | String.t(), number()) :: :ok
  def set(name, value) do
    init()
    :ets.insert(@table, {{:gauge, name}, value})
    :ok
  end

  @doc "Record a latency observation (milliseconds) into a histogram."
  @spec observe(atom() | String.t(), number()) :: :ok
  def observe(name, value) do
    init()
    key = {:hist, name}

    case :ets.lookup(@table, key) do
      [{^key, count, sum, minv, maxv}] ->
        :ets.insert(@table, {key, count + 1, sum + value, min(minv, value), max(maxv, value)})

      [] ->
        :ets.insert(@table, {key, 1, value, value, value})
    end

    :ok
  end

  @doc "Read a single metric value (counter/gauge) or histogram tuple."
  @spec get(atom() | String.t()) :: term()
  def get(name) do
    init()
    lookup_counter(name) || lookup_gauge(name) || lookup_hist(name)
  end

  defp lookup_counter(name) do
    case :ets.lookup(@table, {:counter, name}) do
      [{_, v}] -> v
      _ -> nil
    end
  end

  defp lookup_gauge(name) do
    case :ets.lookup(@table, {:gauge, name}) do
      [{{:gauge, ^name}, v}] -> v
      _ -> nil
    end
  end

  defp lookup_hist(name) do
    case :ets.lookup(@table, {:hist, name}) do
      [{{:hist, ^name}, c, s, mn, mx}] -> %{count: c, sum: s, min: mn, max: mx}
      _ -> nil
    end
  end

  @doc "Read all metrics as a map."
  @spec all() :: map()
  def all do
    init()

    :ets.tab2list(@table)
    |> Enum.reduce(%{}, fn
      {{:counter, name}, v}, acc ->
        Map.put(acc, {:counter, name}, v)

      {{:gauge, name}, v}, acc ->
        Map.put(acc, {:gauge, name}, v)

      {{:hist, name}, c, s, mn, mx}, acc ->
        Map.put(acc, {:hist, name}, %{count: c, sum: s, min: mn, max: mx})

      _, acc ->
        acc
    end)
  end

  @doc "Reset all metrics."
  @spec reset() :: :ok
  def reset do
    init()
    :ets.delete_all_objects(@table)
    :ok
  end
end
