defmodule RaftEx.LogSnapshot do
  @behaviour RaftEx.Snapshot

  @magic "RASN"
  @version 1

  @impl RaftEx.Snapshot
  def prepare(_index, state), do: state

  @impl RaftEx.Snapshot
  def write(dir, meta, mac_state, sync) do
    meta_bin = :erlang.term_to_binary(meta)
    iovec = :erlang.term_to_iovec(mac_state)
    data = [<<byte_size(meta_bin)::32-unsigned>>, meta_bin | iovec]
    checksum = :erlang.crc32(data)
    file = filename(dir)
    bytes = 9 + :erlang.iolist_size(data)

    header = <<@magic, @version::8-unsigned, checksum::32-integer>>

    case RaftEx.Lib.write_file(file, [header | data], sync) do
      :ok -> {:ok, bytes}
      err -> err
    end
  end

  @impl RaftEx.Snapshot
  def sync(dir) do
    RaftEx.File.sync_file(filename(dir))
  end

  @impl RaftEx.Snapshot
  def begin_accept(snap_dir, meta) do
    file = filename(snap_dir)
    {:ok, fd} = :file.open(file, [:write, :binary, :raw])
    meta_bin = :erlang.term_to_binary(meta)
    data = [<<byte_size(meta_bin)::32-unsigned>>, meta_bin]
    partial_crc = :erlang.crc32(data)
    chunk = [<<@magic, @version::8-unsigned, 0::32-integer>>, data]
    bytes = :erlang.iolist_size(chunk)
    :ok = :file.write(fd, chunk)
    {:ok, {bytes, partial_crc, fd}}
  end

  @impl RaftEx.Snapshot
  def accept_chunk(
        <<@magic, @version::8-unsigned, crc::32-integer, rest::binary>>,
        {_bytes, _partial_crc, fd}
      ) do
    partial_crc = :erlang.crc32(rest)
    bytes = 5 + 4 + byte_size(rest)
    {:ok, 0} = :file.position(fd, 0)
    :ok = :file.write(fd, <<@magic, @version::8-unsigned, crc::32-integer, rest::binary>>)
    {:ok, {bytes, partial_crc, crc, fd}}
  end

  def accept_chunk(chunk, {bytes, partial_crc, fd}) do
    <<_crc::32-integer, rest::binary>> = chunk
    accept_chunk(rest, {bytes, partial_crc, nil, fd})
  end

  def accept_chunk(chunk, {bytes, partial_crc0, crc, fd}) do
    bytes1 = bytes + byte_size(chunk)
    :ok = :file.write(fd, chunk)
    partial_crc = :erlang.crc32(partial_crc0, chunk)
    {:ok, {bytes1, partial_crc, crc, fd}}
  end

  @impl RaftEx.Snapshot
  def complete_accept(chunk, {bytes, _partial_crc, crc0, fd}) do
    {:ok, {final_bytes, calculated_crc, crc0, _fd}} = accept_chunk(chunk, {bytes, 0, crc0, fd})
    crc_to_write = if crc0 == nil, do: calculated_crc, else: crc0
    :ok = :file.pwrite(fd, 5, <<crc_to_write::32-integer>>)
    :ok = RaftEx.File.sync(fd)
    :ok = :file.close(fd)
    {:ok, final_bytes}
  end

  @impl RaftEx.Snapshot
  def begin_read(dir, context) do
    file = filename(dir)

    with {:ok, fd} <- :file.open(file, [:read, :binary, :raw]),
         {:ok, meta, crc} <- read_meta_internal(fd) do
      {:ok, eof} = :file.position(fd, :eof)

      if Map.get(context, :can_accept_full_file) do
        {:ok, meta, {0, eof, fd}}
      else
        {:ok, cur} = :file.position(fd, :cur)
        {:ok, meta, {crc, {cur, eof, fd}}}
      end
    else
      err ->
        err
    end
  end

  @impl RaftEx.Snapshot
  def read_chunk({crc, read_state}, size, dir) when is_integer(crc) do
    case read_chunk(read_state, size - 4, dir) do
      {:ok, data, next} -> {:ok, <<crc::32-integer, data::binary>>, next}
      err -> err
    end
  end

  def read_chunk({pos, eof, fd}, size, _dir) do
    case :file.pread(fd, pos, size) do
      {:ok, data} ->
        if pos + size >= eof do
          :file.close(fd)
          {:ok, data, :last}
        else
          {:ok, data, {:next, {pos + size, eof, fd}}}
        end

      {:error, _} = err ->
        err

      :eof ->
        {:error, :unexpected_eof}
    end
  end

  @impl RaftEx.Snapshot
  def recover(dir) do
    file = filename(dir)

    case :prim_file.read_file(file) do
      {:ok, <<@magic, @version::8-unsigned, crc::32-integer, data::binary>>} ->
        validate(crc, data)

      {:ok, <<@magic, version::8-unsigned, _::binary>>} ->
        {:error, {:invalid_version, version}}

      {:ok, _} ->
        {:error, :invalid_format}

      err ->
        err
    end
  end

  @impl RaftEx.Snapshot
  def validate(dir) do
    case recover(dir) do
      {:ok, _, _} -> :ok
      err -> err
    end
  end

  @impl RaftEx.Snapshot
  def read_meta(dir) do
    file = filename(dir)

    case :file.open(file, [:read, :binary, :raw]) do
      {:ok, fd} ->
        result = read_meta_internal(fd)
        :file.close(fd)

        case result do
          {:ok, meta, _crc} -> {:ok, meta}
          err -> err
        end

      err ->
        err
    end
  end

  @impl RaftEx.Snapshot
  def context, do: %{can_accept_full_file: true}

  @impl RaftEx.Snapshot
  def get_size(dir) do
    file = filename(dir)

    case :prim_file.read_file_info(file) do
      {:ok, info} -> {:ok, elem(info, 1)}
      err -> err
    end
  end

  # ---- private -------------------------------------------------------

  defp filename(dir), do: Path.join(dir, "snapshot.dat")

  defp read_meta_internal(fd) do
    header_size = 9 + 4

    case :file.read(fd, header_size) do
      {:ok, <<@magic, @version::8-unsigned, crc::32-integer, meta_size::32-unsigned>>} ->
        case :file.read(fd, meta_size) do
          {:ok, meta_bin} -> {:ok, :erlang.binary_to_term(meta_bin), crc}
          err -> err
        end

      {:ok, <<@magic, version::8-unsigned, _::binary>>} ->
        {:error, {:invalid_version, version}}

      {:ok, _} ->
        {:error, :invalid_format}

      :eof ->
        {:error, :unexpected_eof_when_parsing_header}

      err ->
        err
    end
  end

  defp validate(crc, data) do
    case :erlang.crc32(data) do
      ^crc -> parse_snapshot(data)
      _ -> {:error, :checksum_error}
    end
  end

  defp parse_snapshot(<<meta_size::32-unsigned, meta_bin::binary-size(meta_size), rest::binary>>) do
    meta = :erlang.binary_to_term(meta_bin)
    {:ok, meta, :erlang.binary_to_term(rest)}
  end
end
