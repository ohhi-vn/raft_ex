
defmodule RaftEx.LeakyIntegrator do
  @moduledoc """
  Exponential-decay leaky integrator for rate estimation.

  Useful for computing a smoothed throughput figure (e.g. commands/second)
  without keeping a full sliding window.  The integrator decays toward zero
  over `decay_time_ms` milliseconds.

  ## Usage

      iex> li = RaftEx.LeakyIntegrator.new(1_000)   # 1-second half-life
      iex> li = RaftEx.LeakyIntegrator.update(10, li)
      iex> RaftEx.LeakyIntegrator.rate(li)
      # → ~10 commands/second immediately after the update
  """

  defstruct [:decay_time_ms, value: 0.0, last_update: nil]

  @type t :: %__MODULE__{
          decay_time_ms: pos_integer(),
          value:         float(),
          last_update:   integer() | nil
        }

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc "Create a new integrator with the given decay time in milliseconds."
  @spec new(pos_integer()) :: t()
  def new(decay_time_ms) when is_integer(decay_time_ms) and decay_time_ms > 0 do
    %__MODULE__{decay_time_ms: decay_time_ms}
  end

  # ---------------------------------------------------------------------------
  # Update
  # ---------------------------------------------------------------------------

  @doc "Add `amount` to the integrator using the current monotonic clock."
  @spec update(number(), t()) :: t()
  def update(amount, state), do: update(amount, now_ms(), state)

  @doc "Add `amount` to the integrator at the given timestamp `ts`."
  @spec update(number(), integer(), t()) :: t()
  def update(amount, ts, %{last_update: nil} = state) do
    %{state | value: to_float(amount), last_update: ts}
  end

  def update(amount, ts, %{decay_time_ms: dtm, value: v, last_update: last} = state)
      when ts >= last do
    elapsed = ts - last
    %{state | value: decay(v, elapsed, dtm) + to_float(amount), last_update: ts}
  end

  # Clock went backwards — just add without decay.
  def update(amount, _ts, %{value: v} = state) do
    %{state | value: v + to_float(amount)}
  end

  # ---------------------------------------------------------------------------
  # Read
  # ---------------------------------------------------------------------------

  @doc "Read the current decayed value using the current monotonic clock."
  @spec read(t()) :: float()
  def read(state), do: read(now_ms(), state)

  @doc "Read the current decayed value at timestamp `ts`."
  @spec read(integer(), t()) :: float()
  def read(_ts, %{last_update: nil}), do: 0.0

  def read(ts, %{decay_time_ms: dtm, value: v, last_update: last}) do
    elapsed = max(0, ts - last)
    decay(v, elapsed, dtm)
  end

  # ---------------------------------------------------------------------------
  # Rate
  # ---------------------------------------------------------------------------

  @doc "Estimated rate in units/second using the current clock."
  @spec rate(t()) :: float()
  def rate(state), do: rate(now_ms(), state)

  @doc "Estimated rate in units/second at timestamp `ts`."
  @spec rate(integer(), t()) :: float()
  def rate(ts, %{decay_time_ms: dtm} = state) do
    read(ts, state) / dtm * 1_000
  end

  # ---------------------------------------------------------------------------
  # Reset
  # ---------------------------------------------------------------------------

  @doc "Reset the integrator to zero."
  @spec reset(t()) :: t()
  def reset(state), do: %{state | value: 0.0, last_update: nil}

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp now_ms, do: :erlang.monotonic_time(:millisecond)

  defp decay(v, elapsed, _dtm) when elapsed <= 0, do: v
  defp decay(v, elapsed, dtm),                     do: v * :math.exp(-elapsed / dtm)

  defp to_float(n) when is_integer(n), do: n / 1
  defp to_float(n) when is_float(n),   do: n
end
