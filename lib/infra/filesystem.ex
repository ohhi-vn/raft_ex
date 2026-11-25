defmodule RaftEx.Infra.Filesystem do
  @moduledoc "Filesystem helpers."

  require Logger

  @retry_delay 20

  @doc "Create `dir` and all parent directories; treat `:eexist` as success."
  @spec make_dir(Path.t()) :: :ok | {:error, term()}
  def make_dir(dir) do
    case File.mkdir_p(dir) do
      :ok -> :ok
      {:error, :eexist} -> :ok
      err -> err
    end
  end

  @doc "Recursively delete `dir`. Raises on failure."
  @spec recursive_delete(Path.t()) :: :ok
  def recursive_delete(dir) do
    case File.rm_rf(dir) do
      {:ok, _} -> :ok
      {:error, reason, _} -> throw({:error, reason})
    end
  end

  @doc """
  Write `io_data` to `name`.

  When `sync` is `true` (default), the file is fsynced after writing to
  ensure data reaches durable storage.
  """
  @spec write_file(Path.t(), iodata(), boolean()) :: :ok | {:error, term()}
  def write_file(name, io_data, sync \\ true) do
    with :ok <- File.write(name, io_data) do
      if sync, do: sync_file(name), else: :ok
    end
  end

  @doc "Ensure the parent directory of `path` exists."
  @spec ensure_dir(Path.t()) :: :ok | {:error, term()}
  def ensure_dir(path), do: File.mkdir_p(Path.dirname(path))

  @doc "Return `true` iff `path` is an existing directory."
  @spec dir?(Path.t()) :: boolean()
  def dir?(path) do
    match?(
      {:ok, {:file_info, _, :directory, _, _, _, _, _, _, _, _, _, _, _}},
      :prim_file.read_file_info(path)
    )
  end

  @doc "Return `true` iff `path` is an existing regular file."
  @spec file?(Path.t()) :: boolean()
  def file?(path) do
    match?(
      {:ok, {:file_info, _, :regular, _, _, _, _, _, _, _, _, _, _, _}},
      :prim_file.read_file_info(path)
    )
  end

  @doc "Return `true` iff `path` exists (any type)."
  @spec any_file?(Path.t()) :: boolean()
  def any_file?(path) do
    match?({:ok, _}, :prim_file.read_file_info(path))
  end

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
