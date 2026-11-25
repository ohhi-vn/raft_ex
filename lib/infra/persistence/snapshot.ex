defmodule RaftEx.Infra.Persistence.Snapshot do
  @moduledoc """
  Behaviour implemented by snapshot storage backends.

  Defines the lifecycle for writing, streaming and recovering snapshots:
  prepare/write/sync on the writer side and begin_read/read_chunk plus
  begin_accept/accept_chunk/complete_accept on the reader side.
  """

  @callback prepare(index :: non_neg_integer(), state :: term()) :: ref :: term()
  @callback write(location :: String.t(), meta :: map(), ref :: term(), sync :: boolean()) ::
              :ok | {:ok, bytes :: non_neg_integer()} | {:error, term()}
  @callback sync(location :: String.t()) :: :ok | {:error, term()}
  @callback begin_read(location :: String.t(), context :: map()) ::
              {:ok, meta :: map(), read_state :: term()} | {:error, term()}
  @callback read_chunk(
              read_state :: term(),
              chunk_bytes :: non_neg_integer(),
              location :: String.t()
            ) ::
              {:ok, chunk :: term(), {:next, term()} | :last} | {:error, term()}
  @callback begin_accept(snap_dir :: String.t(), meta :: map()) ::
              {:ok, accept_state :: term()} | {:error, term()}
  @callback accept_chunk(chunk :: term(), accept_state :: term()) ::
              {:ok, term()} | {:error, term()}
  @callback complete_accept(chunk :: term(), accept_state :: term()) ::
              :ok | {:ok, non_neg_integer()} | {:error, term()}
  @callback recover(location :: String.t()) :: {:ok, map(), term()} | {:error, term()}
  @callback validate(location :: String.t()) :: :ok | {:error, term()}
  @callback read_meta(location :: String.t()) :: {:ok, map()} | {:error, term()}
  @callback context() :: map()
  @callback get_size(location :: String.t()) :: {:ok, non_neg_integer()} | {:error, term()}

  @optional_callbacks [context: 0, get_size: 1]
end
