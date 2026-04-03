defmodule RaftEx.File do
  require Logger

  @retry_delay 20

  def sync(fd) do
    retry_on_error(fn -> :file.sync(fd) end)
  end

  def sync_file(path) do
    case :file.open(path, [:binary, :read, :write, :raw]) do
      {:ok, fd} ->
        result = sync(fd)
        :file.close(fd)
        result

      err ->
        err
    end
  end

  def rename(src, dst) do
    retry_on_error(fn -> :prim_file.rename(src, dst) end)
  end

  defp retry_on_error(op) do
    case op.() do
      {:error, reason} when reason in [:eagain, :eacces] ->
        Logger.debug("File error #{reason}, retrying in #{@retry_delay}ms")
        Process.sleep(@retry_delay)
        op.()

      result ->
        result
    end
  end
end
