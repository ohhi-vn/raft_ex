defmodule RaftEx.Infra.Persistence.LogSnapshot do
  @moduledoc """
  File-backed snapshot storage for the replicated log.

  Snapshots are written as a single `snapshot.dat` file consisting of a
  magic/version/checksum header, a serialized metadata term, and the
  serialized machine state with a final CRC32 checksum.
  """

  @behaviour RaftEx.Infra.Persistence.Snapshot

  alias RaftEx.Infra.Filesystem

  @magic "RASN"
  @version 1

  @impl RaftEx.Infra.Persistence.Snapshot
  def prepare(_index, state), do: state

  @impl RaftEx.Infra.Persistence.Snapshot
  def write(dir, meta, mac_state, sync) do
    meta_bin = :erlang.term_to_binary(meta)
    iovec = :erlang.term_to_iovec(mac_state)
    data = [<<byte_size(meta_bin)::32-unsigned>>, meta_bin | iovec]
    checksum = :erlang.crc32(data)
    file = filename(dir)
    temp = temp_filename(dir)
    bytes = 9 + :erlang.iolist_size(data)

    header = <<@magic, @version::8-unsigned, checksum::32-integer>>

    with :ok <- Filesystem.make_dir(dir),
         :ok <- Filesystem.write_file(temp, [header | data], sync),
         :ok <- Filesystem.rename(temp, file),
         :ok <- sync_dir(dir) do
      {:ok, bytes}
    else
      {:error, _} = err ->
        File.rm(temp)
        err
    end
  end

  @impl RaftEx.Infra.Persistence.Snapshot
  def sync(dir) do
    Filesystem.sync_file(filename(dir))
  end

  @impl RaftEx.Infra.Persistence.Snapshot
  def begin_accept(snap_dir, meta) do
    temp = temp_filename(snap_dir)

    with :ok <- Filesystem.make_dir(snap_dir),
         {:ok, fd} <- :file.open(temp, [:write, :binary, :raw]) do
      meta_bin = :erlang.term_to_binary(meta)
      data = [<<byte_size(meta_bin)::32-unsigned>>, meta_bin]
      partial_crc = :erlang.crc32(data)
      chunk = [<<@magic, @version::8-unsigned, 0::32-integer>>, data]
      bytes = :erlang.iolist_size(chunk)

      case :file.write(fd, chunk) do
        :ok -> {:ok, {bytes, partial_crc, temp, fd}}
        {:error, _} = err -> abort(fd, temp, err)
      end
    else
      {:error, _} = err -> err
    end
  end

  @doc """
  Abandon an in-flight transfer: close its file descriptor and delete only
  the temporary file. The previous valid snapshot, if any, is preserved.
  """
  @spec abort(term()) :: :ok | {:error, term()}
  def abort({_, _, temp, fd}), do: abort(fd, temp, :ok)
  def abort({_, _, _, temp, fd}), do: abort(fd, temp, :ok)
  def abort(_), do: :ok

  defp abort(fd, temp, result) do
    try do
      :file.close(fd)
    catch
      _, _ -> :ok
    end

    File.rm(temp)
    result
  end

  @impl RaftEx.Infra.Persistence.Snapshot
  def accept_chunk(
        <<@magic, @version::8-unsigned, crc::32-integer, rest::binary>>,
        {_bytes, _partial_crc, temp, fd}
      ) do
    partial_crc = :erlang.crc32(rest)
    bytes = 5 + 4 + byte_size(rest)

    with {:ok, 0} <- :file.position(fd, 0),
         :ok <- :file.write(fd, <<@magic, @version::8-unsigned, crc::32-integer, rest::binary>>) do
      {:ok, {bytes, partial_crc, crc, temp, fd}}
    else
      {:error, _reason} = error -> error
    end
  end

  def accept_chunk(<<_crc::32-integer, rest::binary>>, {bytes, partial_crc, temp, fd}) do
    bytes1 = bytes + byte_size(rest)

    case :file.write(fd, rest) do
      :ok -> {:ok, {bytes1, :erlang.crc32(partial_crc, rest), nil, temp, fd}}
      {:error, _reason} = error -> error
    end
  end

  def accept_chunk(_chunk, {_bytes, _partial_crc, _temp, _fd}), do: {:error, :invalid_chunk}

  # Every wire chunk is framed as `<<crc::32, payload>>`; strip the frame so
  # no CRC bytes can leak into the snapshot. Anything shorter than a frame
  # is rejected.
  def accept_chunk(<<_crc::32-integer, rest::binary>>, {bytes, partial_crc0, crc, temp, fd}) do
    bytes1 = bytes + byte_size(rest)

    case :file.write(fd, rest) do
      :ok ->
        {:ok, {bytes1, :erlang.crc32(partial_crc0, rest), crc, temp, fd}}

      {:error, _reason} = error ->
        error
    end
  end

  def accept_chunk(_chunk, {_bytes, _partial_crc0, _crc, _temp, _fd}),
    do: {:error, :invalid_chunk}

  # Defensive fallback: a snapshot_state that is neither nil nor an accept
  # tuple can never occur from the first-chunk path, but failing closed
  # keeps a corrupt message from crashing the follower.
  def accept_chunk(_chunk, _accept_state), do: {:error, :invalid_chunk}

  @impl RaftEx.Infra.Persistence.Snapshot
  def complete_accept(chunk, {bytes, partial_crc, crc0, temp, fd}) do
    # The final chunk carries the same `<<crc::32, payload>>` framing as
    # every other chunk; `accept_chunk/2` strips it uniformly.
    case accept_chunk(chunk, {bytes, partial_crc, crc0, temp, fd}) do
      {:ok, {final_bytes, calculated_crc, crc0, temp, _fd}} ->
        crc_to_write = if crc0 == nil, do: calculated_crc, else: crc0

        result =
          with :ok <- :file.pwrite(fd, 5, <<crc_to_write::32-integer>>),
               :ok <- Filesystem.sync(fd),
               :ok <- :file.close(fd),
               :ok <- Filesystem.rename(temp, snapshot_file_of(temp)),
               :ok <- sync_dir(snapshot_file_of(temp)) do
            {:ok, final_bytes}
          else
            {:error, _reason} = error ->
              abort(fd, temp, error)
          end

        result

      {:error, _reason} = error ->
        abort(fd, temp, error)
    end
  end

  @impl RaftEx.Infra.Persistence.Snapshot
  def begin_read(dir, context) do
    file = filename(dir)

    with {:ok, fd} <- :file.open(file, [:read, :binary, :raw]),
         {:ok, meta, crc} <- read_meta_internal(fd) do
      {:ok, cur} = :file.position(fd, :cur)
      {:ok, eof} = :file.position(fd, :eof)

      if Map.get(context, :can_accept_full_file) do
        {:ok, meta, {0, eof, fd}}
      else
        {:ok, meta, {crc, {cur, eof, fd}}}
      end
    else
      err ->
        err
    end
  end

  @impl RaftEx.Infra.Persistence.Snapshot
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

  @impl RaftEx.Infra.Persistence.Snapshot
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

  @impl RaftEx.Infra.Persistence.Snapshot
  def validate(dir) do
    case recover(dir) do
      {:ok, _, _} -> :ok
      err -> err
    end
  end

  @impl RaftEx.Infra.Persistence.Snapshot
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

  @impl RaftEx.Infra.Persistence.Snapshot
  def context, do: %{can_accept_full_file: true}

  @impl RaftEx.Infra.Persistence.Snapshot
  def get_size(dir) do
    file = filename(dir)

    case :prim_file.read_file_info(file) do
      {:ok, info} -> {:ok, elem(info, 1)}
      err -> err
    end
  end

  # ---- private -------------------------------------------------------

  defp filename(dir), do: Path.join(dir, "snapshot.dat")

  defp temp_filename(dir), do: Path.join(dir, "snapshot.dat.tmp")

  defp snapshot_file_of(temp) do
    if String.ends_with?(temp, ".tmp") do
      binary_part(temp, 0, byte_size(temp) - 4)
    else
      temp
    end
  end

  # Best-effort directory sync so a rename survives power loss where the
  # platform allows it. The file driver refuses to open directories
  # (`:eisdir`); the rename itself remains atomic regardless.
  defp sync_dir(file_path) do
    dir = Path.dirname(file_path)

    try do
      case :file.open(dir, [:read, :binary, :raw]) do
        {:ok, dir_fd} ->
          result = :file.sync(dir_fd)
          :file.close(dir_fd)
          result

        {:error, :eisdir} ->
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    catch
      _, reason -> {:error, reason}
    end
  end

  defp read_meta_internal(fd) do
    header_size = 9 + 4

    case read_exact(fd, header_size) do
      {:ok, <<@magic, @version::8-unsigned, crc::32-integer, meta_size::32-unsigned>>} ->
        read_meta_body(fd, meta_size, crc)

      {:ok, <<@magic, version::8-unsigned, _::binary>>} ->
        {:error, {:invalid_version, version}}

      {:ok, _} ->
        {:error, :invalid_format}

      :eof ->
        {:error, :unexpected_eof_when_parsing_header}

      err ->
        err
    end
  rescue
    _ -> {:error, :invalid_format}
  catch
    _, _ -> {:error, :invalid_format}
  end

  defp read_meta_body(fd, meta_size, crc)
       when is_integer(meta_size) and meta_size >= 0 and meta_size <= 64_000_000 do
    case read_exact(fd, meta_size) do
      {:ok, meta_bin} ->
        meta =
          try do
            :erlang.binary_to_term(meta_bin, [:safe])
          rescue
            ArgumentError -> {:error, :invalid_metadata}
          catch
            _, _ -> {:error, :invalid_metadata}
          end

        case meta do
          {:error, _} = err -> err
          _ -> {:ok, meta, crc}
        end

      :eof ->
        {:error, :unexpected_eof_when_parsing_header}

      err ->
        err
    end
  end

  defp read_meta_body(_fd, _meta_size, _crc), do: {:error, :invalid_format}

  defp read_exact(_fd, 0), do: {:ok, <<>>}

  defp read_exact(fd, n) when is_integer(n) and n > 0 do
    read_exact(fd, n, [])
  end

  defp read_exact(_fd, 0, acc), do: {:ok, :erlang.iolist_to_binary(Enum.reverse(acc))}

  defp read_exact(fd, remaining, acc) when remaining > 0 do
    case :file.read(fd, remaining) do
      {:ok, bin} when is_binary(bin) ->
        got = byte_size(bin)

        cond do
          got == remaining ->
            {:ok, :erlang.iolist_to_binary(Enum.reverse([bin | acc]))}

          got > 0 ->
            read_exact(fd, remaining - got, [bin | acc])

          true ->
            read_partial(acc)
        end

      :eof ->
        read_partial(acc)

      {:error, _} = err ->
        err
    end
  end

  # A short read yields what was read; a completely empty read is `:eof` so
  # callers can tell a missing header from a truncated one.
  defp read_partial([]), do: :eof
  defp read_partial(acc), do: {:ok, :erlang.iolist_to_binary(Enum.reverse(acc))}

  defp validate(crc, data) do
    case :erlang.crc32(data) do
      ^crc -> parse_snapshot(data)
      _ -> {:error, :checksum_error}
    end
  rescue
    _ -> {:error, :checksum_error}
  catch
    _, _ -> {:error, :checksum_error}
  end

  defp parse_snapshot(<<meta_size::32-unsigned, rest::binary>>)
       when meta_size >= 0 and meta_size <= 64_000_000 and byte_size(rest) >= meta_size do
    <<meta_bin::binary-size(^meta_size), state_bin::binary>> = rest

    try do
      meta = :erlang.binary_to_term(meta_bin, [:safe])
      mac_state = :erlang.binary_to_term(state_bin, [:safe])

      if is_map(meta) do
        {:ok, meta, mac_state}
      else
        {:error, :invalid_format}
      end
    rescue
      ArgumentError -> {:error, :invalid_format}
    catch
      _, _ -> {:error, :invalid_format}
    end
  end

  defp parse_snapshot(_), do: {:error, :invalid_format}
end
