defmodule RaftEx.Machine do
  @moduledoc """
  Behaviour for RaftEx state machines.

  Implement `c:init/1` and `c:apply/3` as mandatory callbacks.  All other
  callbacks (`tick`, `state_enter`, snapshot hooks, versioning, etc.) are
  optional — sensible defaults are used when they are absent.

  ## Effects

  `c:apply/3` may return a third element containing one or more *effects* that
  the Raft framework will execute after the entry is applied.  Effects are
  described by `t:effect/0`.
  """

  @type state        :: term()
  @type user_command :: term()
  @type milliseconds :: non_neg_integer()
  @type version      :: non_neg_integer()
  @type machine      :: {:machine, module(), map()}
  @type effects      :: [effect()]
  @type reply        :: term()

  @type command_meta :: %{
          system_time: integer(),
          index: non_neg_integer(),
          term: non_neg_integer(),
          optional(:machine_version) => version(),
          optional(:from) => term(),
          optional(:reply_mode) => term()
        }

  @type effect ::
          {:send_msg, to :: term(), msg :: term()}
          | {:send_msg, to :: term(), msg :: term(), opts :: term()}
          | {:mod_call, module(), atom(), [term()]}
          | {:append, cmd :: term()}
          | {:append, cmd :: term(), reply_mode :: term()}
          | {:monitor, :process, pid()}
          | {:monitor, :node, node()}
          | {:demonitor, :process, pid()}
          | {:demonitor, :node, node()}
          | {:timer, term(), non_neg_integer() | :infinity}
          | {:release_cursor, RaftEx.Types.index(), state()}
          | {:release_cursor, RaftEx.Types.index(), state(), map()}
          | {:release_cursor, RaftEx.Types.index()}
          | {:checkpoint, RaftEx.Types.index(), state()}
          | {:aux, term()}
          | :garbage_collection

  # ---------------------------------------------------------------------------
  # Required callbacks
  # ---------------------------------------------------------------------------

  @callback init(conf :: map()) :: state()

  @callback apply(command_meta(), command :: term(), state()) ::
              {state(), reply(), effects() | effect()} | {state(), reply()}

  # ---------------------------------------------------------------------------
  # Optional callbacks
  # ---------------------------------------------------------------------------

  @optional_callbacks [
    tick: 2,
    snapshot_installed: 4,
    state_enter: 2,
    init_aux: 1,
    handle_aux: 5,
    handle_aux: 6,
    overview: 1,
    live_indexes: 1,
    snapshot_module: 0,
    version: 0,
    which_module: 1
  ]

  @callback tick(milliseconds(), state()) :: effects()
  @callback snapshot_installed(meta :: map(), state(), old_meta :: map(), old_state :: state()) ::
              effects()
  @callback state_enter(ra_state :: atom(), state()) :: effects()
  @callback init_aux(name :: atom()) :: term()
  @callback handle_aux(
              ra_state :: atom(),
              type :: term(),
              cmd :: term(),
              aux_state :: term(),
              internal :: term()
            ) ::
              {:reply, term(), term(), term()}
              | {:reply, term(), term(), term(), effects()}
              | {:no_reply, term(), term()}
              | {:no_reply, term(), term(), effects()}
  @callback handle_aux(
              ra_state :: atom(),
              type :: term(),
              cmd :: term(),
              aux_state :: term(),
              log :: term(),
              mac_state :: state()
            ) ::
              {:reply, term(), term(), term()} | {:no_reply, term(), term()}
  @callback overview(state()) :: map()
  @callback live_indexes(state()) :: [RaftEx.Types.index()] | {:ra_seq, term()}
  @callback snapshot_module() :: module()
  @callback version() :: version()
  @callback which_module(version()) :: module()

  @default_version 0

  # ---------------------------------------------------------------------------
  # Public helpers — delegate to machine module, swallowing UndefinedFunctionError
  # ---------------------------------------------------------------------------

  @spec init(machine(), atom(), version()) :: state()
  def init({:machine, _mod, args} = machine, name, version) do
    mod = which_module(machine, version)
    mod.init(Map.merge(args, %{name: name, machine_version: version}))
  end

  @spec apply(module(), command_meta(), term(), state()) :: {state(), reply(), effects()}
  def apply(mod, metadata, cmd, state) do
    case mod.apply(metadata, cmd, state) do
      {s, r, e} -> {s, r, e}
      {s, r}    -> {s, r, []}
    end
  end

  @spec tick(module(), milliseconds(), state()) :: effects()
  def tick(mod, time_ms, state) do
    call_optional(fn -> mod.tick(time_ms, state) end, [])
  end

  @spec snapshot_installed(module(), map(), state(), map(), state()) :: effects()
  def snapshot_installed(mod, meta, state, old_meta, old_state) do
    call_optional(
      fn -> mod.snapshot_installed(meta, state, old_meta, old_state) end,
      fn ->
        call_optional(fn -> mod.snapshot_installed(meta, state) end, [])
      end
    )
  end

  @spec state_enter(module(), atom(), state()) :: effects()
  def state_enter(mod, raft_state, state) do
    call_optional(fn -> mod.state_enter(raft_state, state) end, [])
  end

  @spec overview(module(), state()) :: map()
  def overview(mod, state) do
    call_optional(fn -> mod.overview(state) end, state)
  end

  @spec live_indexes(module(), state()) :: term()
  def live_indexes(mod, state) do
    result = call_optional(fn -> mod.live_indexes(state) end, [])

    case result do
      {:ra_seq, seq} -> seq
      list           -> RaftEx.Seq.from_list(list)
    end
  end

  @spec version(module() | machine()) :: version()
  def version(mod) when is_atom(mod) do
    call_optional(fn -> assert_version(mod.version()) end, @default_version)
  end

  def version({:machine, mod, _}), do: version(mod)

  @spec is_versioned(machine()) :: boolean()
  def is_versioned({:machine, mod, _}) do
    call_optional(fn -> mod.version() && true end, false)
  end

  @spec which_module(machine(), version()) :: module()
  def which_module({:machine, mod, _}, version) do
    call_optional(fn -> mod.which_module(version) end, mod)
  end

  @spec init_aux(module(), atom()) :: term()
  def init_aux(mod, name) do
    call_optional(fn -> mod.init_aux(name) end, nil)
  end

  @spec handle_aux(module(), atom(), term(), term(), term(), term(), term()) :: term()
  def handle_aux(mod, raft_state, type, cmd, aux, log, mac_state) do
    mod.handle_aux(raft_state, type, cmd, aux, log, mac_state)
  end

  @spec handle_aux(module(), atom(), term(), term(), term(), term()) :: term()
  def handle_aux(mod, raft_state, type, cmd, aux, state) do
    mod.handle_aux(raft_state, type, cmd, aux, state)
  end

  @doc """
  Determine which arity of `handle_aux` the module exports (5 or 6 args),
  returning `{:handle_aux, arity}` or `nil` if neither is exported.
  """
  @spec which_aux_fun(module()) :: {:handle_aux, 5 | 6} | nil
  def which_aux_fun(mod) when is_atom(mod) do
    exports = mod.__info__(:functions)

    exports
    |> Enum.filter(fn {name, _arity} -> name == :handle_aux end)
    |> Enum.sort()
    |> case do
      []          -> nil
      [{_, _} = h | _] -> h
    end
  end

  @spec query(module(), fun() | {module(), atom(), list()}, state(), map()) :: term()
  def query(mod, fun, state, ctx \\ %{})

  def query(mod, fun, state, ctx) when mod != RaftEx.MachineSimple do
    apply_query_fun(fun, state, ctx)
  end

  def query(RaftEx.MachineSimple, fun, {:simple, _, state}, ctx) do
    apply_query_fun(fun, state, ctx)
  end

  @spec module(machine()) :: module()
  def module({:machine, mod, _}), do: mod

  @spec snapshot_module(machine()) :: module()
  def snapshot_module({:machine, mod, _}) do
    call_optional(fn -> mod.snapshot_module() end, RaftEx.LogSnapshot)
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Call `fun`, returning `default` if `UndefinedFunctionError` is raised.
  # `default` may itself be a zero-arity function for lazy evaluation.
  defp call_optional(fun, default) do
    fun.()
  rescue
    UndefinedFunctionError ->
      if is_function(default, 0), do: default.(), else: default
  end

  defp apply_query_fun(fun, state, ctx) when is_function(fun, 2), do: fun.(ctx, state)
  defp apply_query_fun(fun, state, _ctx) when is_function(fun, 1), do: fun.(state)

  defp apply_query_fun({m, f, a, opts}, state, ctx) when is_list(opts) do
    args = if :with_context in opts, do: a ++ [ctx, state], else: a ++ [state]
    apply(m, f, args)
  end

  defp apply_query_fun({m, f, a}, state, _ctx), do: apply(m, f, a ++ [state])

  defp assert_version(i) when is_integer(i) and i >= 0, do: i
end
