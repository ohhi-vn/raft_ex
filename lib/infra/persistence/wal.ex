defmodule RaftEx.Infra.Persistence.Wal do
  @moduledoc """
  Behaviour for RaftEx write-ahead log implementations.

  Two implementations are planned:
    * `RaftEx.Infra.Persistence.LogWal` (per-server files, default)
    * a future shared-WAL implementation for many consensus groups

  Implementations must be safe to call concurrently for different uids.
  """

  @type uid :: term()
  @type entry :: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.term_num(), term()}
  @type wal_name :: atom()

  @callback append(wal_name, uid, [entry]) ::
              {:ok, RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index()}
              | {:ok, non_neg_integer(), non_neg_integer()}
  @callback append_async(wal_name, uid, [entry], pid()) :: :ok
  @callback read(wal_name, uid, RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index()) :: [
              entry
            ]
  @callback truncate(wal_name, uid, RaftEx.Domain.Types.index()) :: :ok
  @callback truncate_from(wal_name, uid, RaftEx.Domain.Types.index()) :: :ok
  @callback info(wal_name, uid) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  @callback recover(map()) :: {RaftEx.Domain.Types.index(), RaftEx.Domain.Types.index(), [entry]}
end
