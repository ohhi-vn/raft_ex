defmodule RaftEx.TestSupport.WalFault do
  @moduledoc """
  WAL fault-injection helpers for hardening tests (task 1.3).

  Provides deterministic on-disk corruptions and in-memory failing servers
  so crash-recovery paths can be exercised without real disk failures.
  """

  @doc "Flip a byte at `offset` in the file at `path`."
  def corrupt_byte(path, offset) when is_binary(path) and is_integer(offset) do
    {:ok, bin} = File.read(path)
    at = rem(offset, byte_size(bin))
    before = binary_part(bin, 0, at)
    <<byte, rest::binary>> = binary_part(bin, at, byte_size(bin) - at)
    flipped = Bitwise.bxor(byte, 0xFF)
    File.write!(path, <<before::binary, flipped, rest::binary>>)
    :ok
  end

  @doc "Truncate the file at `path` to `length` bytes (simulates a torn write)."
  def truncate_file(path, length) when is_binary(path) and is_integer(length) do
    {:ok, bin} = File.read(path)
    kept = binary_part(bin, 0, min(length, byte_size(bin)))
    File.write!(path, kept)
    :ok
  end

  @doc "Append non-WAL bytes after the last valid record."
  def append_garbage(path, garbage \\ "GARBAGE") do
    File.write!(path, garbage, [:append])
    :ok
  end

  @doc "Start a WAL stand-in that rejects appends with `{:error, reason}`."
  def start_failing_wal(reason \\ :wal_down) do
    {:ok, pid} = GenServer.start_link(__MODULE__.FailingWal, reason, [])
    pid
  end

  defmodule FailingWal do
    @moduledoc false
    use GenServer

    def init(reason), do: {:ok, reason}

    def handle_call({:append, _uid, _entries}, _from, reason),
      do: {:reply, {:error, reason}, reason}

    def handle_call({:truncate, _uid, _idx}, _from, reason),
      do: {:reply, {:error, reason}, reason}

    def handle_call({:truncate_from, _uid, _idx}, _from, reason),
      do: {:reply, {:error, reason}, reason}

    def handle_call(_msg, _from, reason), do: {:reply, {:error, reason}, reason}
  end
end
