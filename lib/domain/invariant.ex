defmodule RaftEx.Domain.Invariant do
  @moduledoc """
  Explicit persistence/recovery index invariants for RaftEx.

  The five indices involved are:

    * `:snapshot_index`     - highest index captured in the latest snapshot
    * `:wal_persisted_index` - highest index durably written to the WAL
    * `:log_last_index`     - highest index present in the in-memory log
    * `:commit_index`       - highest index known to be committed
    * `:last_applied`       - highest index applied to the state machine

  Invariants (all must hold):

    1. `snapshot_index <= wal_persisted_index`
    2. `wal_persisted_index <= log_last_index`
    3. `commit_index <= log_last_index`
    4. `last_applied <= commit_index`
    5. `snapshot_index <= last_applied` (when a snapshot exists, i.e. snapshot_index > 0)

  Checked via `check/1` (returns `:ok` or `{:error, reason}`) and `check!/1`
  (raises `RaftEx.Domain.Invariant.Violation` when checks are enabled). Checks
  are gated by `Application.get_env(:raft_ex, :invariant_checks)` (`:off` by
  default, `:test`/`:debug` to enable).
  """

  defmodule Violation do
    defexception [:reason, :indices]

    @impl true
    def message(%{reason: reason, indices: indices}) do
      "Raft invariant violated: #{inspect(reason)}; indices=#{inspect(indices)}"
    end
  end

  @type indices :: %{
          optional(:snapshot_index) => non_neg_integer(),
          optional(:wal_persisted_index) => non_neg_integer(),
          optional(:log_last_index) => non_neg_integer(),
          optional(:commit_index) => non_neg_integer(),
          optional(:last_applied) => non_neg_integer()
        }

  @doc "Returns the current check mode."
  @spec mode() :: :off | :test | :debug
  def mode do
    Application.get_env(:raft_ex, :invariant_checks, :off)
  end

  @doc "Pure check: returns :ok or {:error, reason}. Enforces all five documented invariants including WAL<=log."
  @spec check(indices()) :: :ok | {:error, term()}
  def check(indices) when is_map(indices) do
    si = Map.get(indices, :snapshot_index, 0)
    lli = Map.get(indices, :log_last_index, 0)
    wpi = Map.get(indices, :wal_persisted_index, lli)
    ci = Map.get(indices, :commit_index, 0)
    la = Map.get(indices, :last_applied, 0)

    cond do
      si > wpi -> {:error, {:snapshot_after_wal, si, wpi}}
      wpi > lli -> {:error, {:wal_after_log, wpi, lli}}
      ci > lli -> {:error, {:commit_after_log, ci, lli}}
      la > ci -> {:error, {:applied_after_commit, la, ci}}
      si > 0 and si > la -> {:error, {:snapshot_after_applied, si, la}}
      true -> :ok
    end
  end

  @doc """
  Recovery-relevant check: WAL may legitimately be ahead of the in-memory log
  (e.g. after truncation of a conflicting suffix before WAL truncation), so
  only the ordering that must hold post-recovery is enforced.
  """
  @spec check_recovery(indices()) :: :ok | {:error, term()}
  def check_recovery(indices) when is_map(indices) do
    si = Map.get(indices, :snapshot_index, 0)
    lli = Map.get(indices, :log_last_index, 0)
    wpi = Map.get(indices, :wal_persisted_index, lli)
    ci = Map.get(indices, :commit_index, 0)
    la = Map.get(indices, :last_applied, 0)

    cond do
      si > wpi -> {:error, {:snapshot_after_wal, si, wpi}}
      ci > lli -> {:error, {:commit_after_log, ci, lli}}
      la > ci -> {:error, {:applied_after_commit, la, ci}}
      si > 0 and si > la -> {:error, {:snapshot_after_applied, si, la}}
      true -> :ok
    end
  end

  @doc "Raises on violation when checks are enabled; no-op when :off. Uses check/1."
  @spec check!(indices()) :: :ok
  def check!(indices), do: do_check(indices, &check/1)

  @doc "Raises on violation when checks are enabled; no-op when :off. Uses check_recovery/1."
  @spec check_recovery!(indices()) :: :ok
  def check_recovery!(indices), do: do_check(indices, &check_recovery/1)

  defp do_check(indices, fun) do
    case mode() do
      :off ->
        :ok

      _ ->
        case fun.(indices) do
          :ok -> :ok
          {:error, reason} -> raise Violation, reason: reason, indices: indices
        end
    end
  end

  @doc "Convenience: build an indices map from a keyword list."
  @spec from(keyword()) :: indices()
  def from(kw) when is_list(kw), do: Map.new(kw)
end
