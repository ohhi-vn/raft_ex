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
        try do
          :ets.new(@table, [
            :named_table,
            :public,
            :set,
            {:write_concurrency, true},
            {:read_concurrency, true}
          ])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end

  @doc "Increment a counter by `amount` (default 1)."
  @spec inc(atom() | String.t(), number()) :: :ok
  def inc(name, amount \\ 1) do
    key = {:counter, name}

    try do
      :ets.update_counter(@table, key, {2, amount}, {key, 0})
      :ok
    rescue
      ArgumentError ->
        init()
        :ets.update_counter(@table, key, {2, amount}, {key, 0})
        :ok
    end
  end

  @doc "Set a gauge value."
  @spec set(atom() | String.t(), number()) :: :ok
  def set(name, value) do
    try do
      :ets.insert(@table, {{:gauge, name}, value})
      :ok
    rescue
      ArgumentError ->
        init()
        :ets.insert(@table, {{:gauge, name}, value})
        :ok
    end
  end

  @doc "Record a latency observation (milliseconds) into a histogram."
  @spec observe(atom() | String.t(), number()) :: :ok
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def observe(name, value) do
    key = {:hist, name}

    try do
      do_observe(key, value)
    rescue
      ArgumentError ->
        init()
        do_observe(key, value)
    end

    :ok
  end

  defp do_observe(key, value) when is_integer(value) do
    # Count and sum are updated atomically; min/max are best-effort.
    :ets.update_counter(@table, key, [{2, 1}, {3, value}], {key, 0, 0, value, value})
    update_hist_extremes(key, value)
  end

  defp do_observe(key, value) do
    case :ets.lookup(@table, key) do
      [{^key, count, sum, minv, maxv}] ->
        :ets.insert(@table, {key, count + 1, sum + value, min(minv, value), max(maxv, value)})

      [] ->
        :ets.insert(@table, {key, 1, value, value, value})
    end
  end

  defp update_hist_extremes(key, value) do
    case :ets.lookup(@table, key) do
      [{^key, _count, _sum, minv, maxv}] ->
        updates = []

        updates = if value < minv, do: [{4, value} | updates], else: updates
        updates = if value > maxv, do: [{5, value} | updates], else: updates

        if updates != [] do
          :ets.update_element(@table, key, updates)
        end

      _ ->
        :ok
    end
  end

  @doc "Read a single metric value (counter/gauge) or histogram tuple."
  @spec get(atom() | String.t()) :: term()
  def get(name) do
    try do
      lookup_counter(name) || lookup_gauge(name) || lookup_hist(name)
    rescue
      ArgumentError -> nil
    end
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
    try do
      :ets.tab2list(@table)
    rescue
      ArgumentError -> []
    end
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
    try do
      :ets.delete_all_objects(@table)
      :ok
    rescue
      ArgumentError -> :ok
    end
  end
end
