defmodule RaftEx.Counters do
  @moduledoc """
  Thin wrapper around the `:seshat` counter library.

  Falls back to no-ops gracefully when `:seshat` is unavailable so that the
  rest of the codebase can call these functions unconditionally.
  """

  @group :ra

  # ---------------------------------------------------------------------------
  # Initialisation
  # ---------------------------------------------------------------------------

  @doc "Initialise the seshat counter group.  Safe to call more than once."
  @spec init() :: :ok
  def init do
    if seshat_available?() do
      Application.ensure_all_started(:seshat)
      :seshat.new_group(@group)
      :persistent_term.put(:ra_seshat_fields_spec, counter_fields())
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Per-server counter management
  # ---------------------------------------------------------------------------

  @spec new(term(), list()) :: :ok
  def new(name, fields_spec) do
    if seshat_available?(), do: :seshat.new(@group, name, fields_spec)
    :ok
  end

  @spec new(term(), list(), map()) :: :ok
  def new(name, fields_spec, labels) do
    if seshat_available?() do
      :seshat.new({@group, name}, fields_spec, labels)
    end
    :ok
  end

  @spec fetch(term()) :: map() | nil
  def fetch(name) do
    if seshat_available?(), do: :seshat.fetch(@group, name), else: nil
  end

  @spec delete(term()) :: :ok
  def delete(name) do
    if seshat_available?(), do: :seshat.delete(@group, name)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Overview / metrics
  # ---------------------------------------------------------------------------

  @spec overview() :: map()
  def overview do
    if seshat_available?(), do: :seshat.counters(@group, :all), else: %{}
  end

  @spec overview(term()) :: map()
  def overview(name) do
    if seshat_available?(), do: :seshat.counters(@group, name), else: %{}
  end

  @spec counters(term(), [atom()]) :: map() | nil
  def counters(name, fields) do
    if seshat_available?(), do: :seshat.counters(@group, name, fields), else: nil
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp seshat_available? do
    Code.ensure_loaded?(:seshat)
  rescue
    _ -> false
  end

  # Extend this list to add new per-server counter fields.
  defp counter_fields, do: []
end
