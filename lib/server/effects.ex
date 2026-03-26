defmodule RaftEx.Server.Effects do
  @moduledoc """
  Pure helpers for building and combining Raft effect lists.

  Effects are accumulated in *reverse* order during log application for
  efficiency, then reversed once at the end.  All functions in this module
  follow that convention unless documented otherwise.
  """

  # ---------------------------------------------------------------------------
  # Reply effects
  # ---------------------------------------------------------------------------

  @doc """
  Append a reply effect (or notification) to the accumulator based on the
  command's `reply_mode`.

  Returns `{effects, notifys}` where `notifys` is a `%{pid => [corr_data]}`
  map of pending pipeline notifications.
  """
  @spec add_reply(map(), term(), term(), list(), map()) :: {list(), map()}

  # No-reply — swallow silently.
  def add_reply(_, :"$ra_no_reply", _, effects, notifys),
    do: {effects, notifys}

  # Await consensus — reply directly to the caller.
  def add_reply(%{from: from}, reply, :await_consensus, effects, notifys) do
    {[{:reply, from, {:wrap_reply, reply}} | effects], notifys}
  end

  # Await consensus with routing options.
  def add_reply(%{from: from}, reply, {:await_consensus, opts}, effects, notifys) do
    replier =
      case opts do
        %{reply_from: :local}       -> :local
        %{reply_from: {:member, m}} -> {:member, m}
        _                           -> :leader
      end

    {[{:reply, from, {:wrap_reply, reply}, replier} | effects], notifys}
  end

  # Pipeline notify — batch into the notifys map for efficiency.
  def add_reply(_, reply, {:notify, corr, pid}, effects, notifys) do
    new_notifys = Map.update(notifys, pid, [{corr, reply}], &[{corr, reply} | &1])
    {effects, new_notifys}
  end

  # Anything else (`:noreply`, unknown modes) — discard.
  def add_reply(_, _, _, effects, notifys), do: {effects, notifys}

  # ---------------------------------------------------------------------------
  # Machine effects
  # ---------------------------------------------------------------------------

  @doc "Prepend machine-emitted effects onto the accumulator."
  @spec append_machine_effects(RaftEx.Machine.effects() | RaftEx.Machine.effect(), list()) :: list()
  def append_machine_effects([], effs),       do: effs
  def append_machine_effects([e], effs),      do: [e | effs]
  def append_machine_effects(app_effs, effs), do: [app_effs | effs]

  # ---------------------------------------------------------------------------
  # Notify effects
  # ---------------------------------------------------------------------------

  @doc """
  Wrap a non-empty `notifys` map into a `{:notify, map}` effect prepended to
  `prior`.  Returns `prior` unchanged when `notifys` is empty.
  """
  @spec make_notify_effects(map(), list()) :: list()
  def make_notify_effects(notifys, prior) when map_size(notifys) > 0,
    do: [{:notify, notifys} | prior]

  def make_notify_effects(_, prior), do: prior

  # ---------------------------------------------------------------------------
  # Error reply effects
  # ---------------------------------------------------------------------------

  @doc "Emit an error reply to the command's `from` field (if present)."
  @spec append_error_reply(tuple(), term(), list()) :: list()
  def append_error_reply({_, %{from: from}, _, _}, reason, effects),
    do: [{:reply, from, {:error, reason}} | effects]

  def append_error_reply(_, _, effects), do: effects

  @doc "Emit an `{idx, term}` acknowledgement for `:after_log_append` commands."
  @spec after_log_append_reply(tuple(), non_neg_integer(), non_neg_integer(), list()) :: list()
  def after_log_append_reply({_, %{from: from}, _, :after_log_append}, idx, term, effects),
    do: [{:reply, from, {:wrap_reply, {idx, term}}} | effects]

  def after_log_append_reply(_, _, _, effects), do: effects
end
