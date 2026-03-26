defmodule RaftEx.Lib do
  @moduledoc """
  General-purpose utilities used across the RaftEx codebase.

  Covers type coercions, UID generation, file helpers, parallel execution,
  and string/path utilities.
  """

  @base64_uri_chars ~c"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-="
  @uid_suffix_length 12
  @uid_chars String.to_charlist("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

  # ---------------------------------------------------------------------------
  # Type coercions
  # ---------------------------------------------------------------------------

  @spec to_list(atom() | binary() | integer() | list()) :: charlist()
  def to_list(a) when is_atom(a),    do: Atom.to_charlist(a)
  def to_list(b) when is_binary(b),  do: String.to_charlist(b)
  def to_list(i) when is_integer(i), do: Integer.to_charlist(i)
  def to_list(l) when is_list(l),    do: l

  @spec to_binary(atom() | binary() | integer() | list()) :: binary()
  def to_binary(b) when is_binary(b),  do: b
  def to_binary(a) when is_atom(a),    do: Atom.to_string(a)
  def to_binary(i) when is_integer(i), do: Integer.to_string(i)
  def to_binary(l) when is_list(l),    do: List.to_string(l)

  @spec to_atom(atom() | binary() | list()) :: atom()
  def to_atom(a) when is_atom(a),   do: a
  def to_atom(b) when is_binary(b), do: String.to_atom(b)
  def to_atom(l) when is_list(l),   do: List.to_atom(l)

  # ---------------------------------------------------------------------------
  # Server-ID helpers
  # ---------------------------------------------------------------------------

  @doc "Extract the local registered name from a `{name, node}` server id."
  @spec ra_server_id_to_local_name(RaftEx.Types.server_id() | atom()) :: atom()
  def ra_server_id_to_local_name({name, _node}), do: name
  def ra_server_id_to_local_name(name) when is_atom(name), do: name

  @doc "Extract the node from a server id, defaulting to `node()` for bare atoms."
  @spec ra_server_id_node(RaftEx.Types.server_id() | atom()) :: node()
  def ra_server_id_node({_name, node}), do: node
  def ra_server_id_node(name) when is_atom(name), do: node()

  # ---------------------------------------------------------------------------
  # UID generation
  # ---------------------------------------------------------------------------

  @doc "Generate a random UID with no prefix."
  @spec make_uid() :: binary()
  def make_uid, do: make_uid(<<>>)

  @doc """
  Generate a UID with an optional binary prefix.

  The prefix is prepended as-is; a random `#{@uid_suffix_length}`-character
  alphanumeric suffix is appended.
  """
  @spec make_uid(binary() | atom() | charlist()) :: binary()
  def make_uid(prefix0) do
    suffix =
      for _ <- 1..@uid_suffix_length, into: "" do
        <<Enum.random(@uid_chars)>>
      end

    to_binary(prefix0) <> suffix
  end

  # ---------------------------------------------------------------------------
  # Validation
  # ---------------------------------------------------------------------------

  @doc "Return `true` iff `str` contains only URL-safe base64 characters."
  @spec validate_base64uri(binary()) :: boolean()
  def validate_base64uri(""), do: false

  def validate_base64uri(str) when is_binary(str) do
    str
    |> String.to_charlist()
    |> Enum.all?(&(&1 in @base64_uri_chars))
  end

  @doc """
  Derive a safe string from `s` by keeping only URL-safe base64 characters,
  truncated to `num` characters.
  """
  @spec derive_safe_string(binary(), pos_integer()) :: binary()
  def derive_safe_string(s, num) do
    s
    |> String.to_charlist()
    |> Enum.filter(&(&1 in @base64_uri_chars))
    |> Enum.take(num)
    |> List.to_string()
  end

  # ---------------------------------------------------------------------------
  # Filesystem helpers
  # ---------------------------------------------------------------------------

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
      if sync, do: RaftEx.File.sync_file(name), else: :ok
    end
  end

  @doc "Ensure the parent directory of `path` exists."
  @spec ensure_dir(Path.t()) :: :ok | {:error, term()}
  def ensure_dir(path), do: File.mkdir_p(Path.dirname(path))

  @doc "Return `true` iff `path` is an existing directory."
  @spec is_dir(Path.t()) :: boolean()
  def is_dir(path) do
    match?(
      {:ok, {:file_info, _, :directory, _, _, _, _, _, _, _, _, _, _, _}},
      :prim_file.read_file_info(path)
    )
  end

  @doc "Return `true` iff `path` is an existing regular file."
  @spec is_file(Path.t()) :: boolean()
  def is_file(path) do
    match?(
      {:ok, {:file_info, _, :regular, _, _, _, _, _, _, _, _, _, _, _}},
      :prim_file.read_file_info(path)
    )
  end

  @doc "Return `true` iff `path` exists (any type)."
  @spec is_any_file(Path.t()) :: boolean()
  def is_any_file(path) do
    match?({:ok, _}, :prim_file.read_file_info(path))
  end

  # ---------------------------------------------------------------------------
  # Number / filename formatting
  # ---------------------------------------------------------------------------

  @doc "Zero-pad `num` to a 16-character uppercase hex string."
  @spec zpad_hex(non_neg_integer()) :: binary()
  def zpad_hex(num) do
    hex = Integer.to_string(num, 16)
    pad = 16 - byte_size(hex)
    if pad > 0, do: String.duplicate("0", pad) <> hex, else: hex
  end

  @spec zpad_filename(binary(), binary(), non_neg_integer()) :: binary()
  def zpad_filename("", ext, num),
    do: :io_lib.format(~c"~16..0B.~ts", [num, ext]) |> List.to_string()

  def zpad_filename(prefix, ext, num),
    do: :io_lib.format(~c"~ts_~16..0B.~ts", [prefix, num, ext]) |> List.to_string()

  @spec zpad_filename_incr(Path.t()) :: Path.t() | nil
  def zpad_filename_incr(fn_) do
    base = Path.basename(fn_)
    dir  = Path.dirname(fn_)

    case Regex.run(~r/^(.*)([0-9]{16})(.*)$/, base, capture: :all_but_first) do
      [prefix, num_str, ext] ->
        new_fn =
          :io_lib.format(~c"~ts~16..0B~ts", [prefix, String.to_integer(num_str) + 1, ext])
          |> List.to_string()

        Path.join(dir, new_fn)

      _ ->
        nil
    end
  end

  @spec zpad_extract_num(binary()) :: non_neg_integer()
  def zpad_extract_num(fn_) do
    [_, num_str, _] = Regex.run(~r/^(.*)([0-9]{16})(.*)$/, fn_, capture: :all_but_first)
    String.to_integer(num_str)
  end

  # ---------------------------------------------------------------------------
  # Concurrency helpers
  # ---------------------------------------------------------------------------

  @doc """
  Apply `fun` to each element of `elements` in parallel, collecting results
  into `{:ok, successes, failures}`.

  `fun` must return `true` (success) or `false` (failure).
  Fails with `{:error, {:partition_parallel_timeout, ...}}` if `timeout` ms elapses.
  """
  @spec partition_parallel((term() -> boolean()), list(), timeout()) ::
          {:ok, list(), list()} | {:error, term()}
  def partition_parallel(fun, elements, timeout \\ 60_000) do
    parent = self()

    tasks =
      Enum.map(elements, fn e ->
        pid_ref = spawn_monitor(fn -> send(parent, {self(), fun.(e)}) end)
        {pid_ref, e}
      end)

    collect(tasks, {[], []}, timeout)
  end

  defp collect([], {successes, failures}, _),
    do: {:ok, successes, failures}

  defp collect([{{pid, mref}, e} | rest], {left, right}, timeout) do
    receive do
      {^pid, true} ->
        Process.demonitor(mref, [:flush])
        collect(rest, {[e | left], right}, timeout)

      {^pid, false} ->
        Process.demonitor(mref, [:flush])
        collect(rest, {left, [e | right]}, timeout)

      {:DOWN, ^mref, :process, ^pid, reason} ->
        collect(rest, {left, [{e, reason} | right]}, timeout)
    after
      timeout -> {:error, {:partition_parallel_timeout, left, right}}
    end
  end

  # ---------------------------------------------------------------------------
  # Misc
  # ---------------------------------------------------------------------------

  @doc "Prepend `item` to `list`."
  @spec cons(term(), list()) :: list()
  def cons(item, list) when is_list(list), do: [item | list]

  @doc "Shuffle a list using Fisher-Yates (via random sort keys)."
  @spec lists_shuffle(list()) :: list()
  def lists_shuffle(list) do
    list
    |> Enum.map(&{:rand.uniform(), &1})
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
