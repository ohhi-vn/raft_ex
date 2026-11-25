defmodule RaftEx.Domain.MachineTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Machine
  alias RaftEx.Domain.Machine.Simple

  defmodule FullMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: Map.put(conf, :initialized, true)

    @impl true
    def apply(_meta, cmd, state), do: {[cmd | state], {:ok, cmd}, [{:aux, cmd}]}

    @impl true
    def tick(ms, _state), do: [{:timer, :tick, ms}]

    @impl true
    def snapshot_installed(_meta, _state, _old_meta, _old_state), do: [{:aux, :installed}]

    @impl true
    def state_enter(ra_state, _state), do: [{:aux, ra_state}]

    @impl true
    def overview(state), do: %{size: length(state)}

    @impl true
    def live_indexes(_state), do: [1, 2, 3]

    @impl true
    def version, do: 3

    @impl true
    def which_module(0), do: RaftEx.Domain.MachineTest.V0Machine
    def which_module(_), do: __MODULE__

    @impl true
    def init_aux(name), do: {:aux, name}

    @impl true
    def handle_aux(_ra_state, _type, cmd, aux, state), do: {:reply, cmd, aux, state}

    @impl true
    def handle_aux(_ra_state, _type, _cmd, aux, _log, mac_state),
      do: {:no_reply, aux, mac_state}

    @impl true
    def snapshot_module, do: RaftEx.Domain.MachineTest.FullSnapshot
  end

  defmodule V0Machine do
    @behaviour Machine

    @impl true
    def init(conf), do: Map.put(conf, :version, 0)

    @impl true
    def apply(_meta, cmd, state), do: {state, cmd}
  end

  defmodule MinimalMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: conf

    @impl true
    def apply(_meta, cmd, state), do: {state, cmd}
  end

  defmodule LegacySnapshotMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: conf

    @impl true
    def apply(_meta, cmd, state), do: {state, cmd}

    def snapshot_installed(_meta, _state), do: [{:aux, :legacy}]
  end

  defmodule RaSeqMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: conf

    @impl true
    def apply(_meta, cmd, state), do: {state, cmd}

    @impl true
    def live_indexes(_state), do: {:ra_seq, :external_seq}
  end

  defmodule FullSnapshot do
  end

  defmodule QueryHelpers do
    def by_state(state), do: {:state, state}
    def by_context(ctx, state), do: {:ctx, ctx, state}
    def with_arg(arg, state), do: {:arg, arg, state}
    def with_arg_ctx(arg, ctx, state), do: {:arg, arg, ctx, state}
  end

  defp metadata do
    %{system_time: 1_000, index: 1, term: 1}
  end

  describe "init/3 and which_module/2" do
    test "initializes through the selected module with name and version merged in" do
      state = Machine.init({:machine, FullMachine, %{arg: 1}}, :server, 1)

      assert state.arg == 1
      assert state.name == :server
      assert state.machine_version == 1
      assert state.initialized
    end

    test "uses which_module/1 to select a versioned implementation" do
      state = Machine.init({:machine, FullMachine, %{}}, :server, 0)
      assert state.version == 0
    end
  end

  describe "apply/4" do
    test "normalizes a two-element callback result" do
      assert {[], :cmd, []} = Machine.apply(MinimalMachine, metadata(), :cmd, [])
    end

    test "keeps a three-element callback result" do
      assert {[:cmd], {:ok, :cmd}, [{:aux, :cmd}]} =
               Machine.apply(FullMachine, metadata(), :cmd, [])
    end
  end

  describe "optional callbacks" do
    test "tick defaults to no effects" do
      assert Machine.tick(MinimalMachine, 10, []) == []
      assert Machine.tick(FullMachine, 10, []) == [{:timer, :tick, 10}]
    end

    test "state_enter defaults to no effects" do
      assert Machine.state_enter(MinimalMachine, :leader, []) == []
      assert Machine.state_enter(FullMachine, :leader, []) == [{:aux, :leader}]
    end

    test "snapshot_installed supports both arities and the default" do
      assert Machine.snapshot_installed(MinimalMachine, %{}, [], %{}, []) == []
      assert Machine.snapshot_installed(FullMachine, %{}, [], %{}, []) == [{:aux, :installed}]

      assert Machine.snapshot_installed(LegacySnapshotMachine, %{}, [], %{}, []) == [
               {:aux, :legacy}
             ]
    end

    test "overview defaults to the state itself" do
      assert Machine.overview(MinimalMachine, %{a: 1}) == %{a: 1}
      assert Machine.overview(FullMachine, [1, 2]) == %{size: 2}
    end

    test "live_indexes converts lists and unwraps ra_seq" do
      assert Machine.live_indexes(FullMachine, []) == [{1, 3}]
      assert Machine.live_indexes(MinimalMachine, []) == []
      assert Machine.live_indexes(RaSeqMachine, []) == :external_seq
    end

    test "init_aux defaults to nil" do
      assert Machine.init_aux(MinimalMachine, :aux) == nil
      assert Machine.init_aux(FullMachine, :aux) == {:aux, :aux}
    end
  end

  describe "version/1, versioned?/1" do
    test "version defaults to zero" do
      assert Machine.version(MinimalMachine) == 0
      assert Machine.version(FullMachine) == 3
      assert Machine.version({:machine, FullMachine, %{}}) == 3
    end

    test "versioned? reflects the callback" do
      refute Machine.versioned?({:machine, MinimalMachine, %{}})
      assert Machine.versioned?({:machine, FullMachine, %{}})
    end
  end

  describe "module/1 and snapshot_module/1" do
    test "module returns the configured module" do
      assert Machine.module({:machine, FullMachine, %{}}) == FullMachine
    end

    test "snapshot_module defaults to the log snapshot module" do
      assert Machine.snapshot_module({:machine, MinimalMachine, %{}}) ==
               RaftEx.Infra.Persistence.LogSnapshot

      assert Machine.snapshot_module({:machine, FullMachine, %{}}) == FullSnapshot
    end
  end

  describe "handle_aux" do
    test "which_aux_fun detects the exported arity" do
      assert Machine.which_aux_fun(FullMachine) == {:handle_aux, 5}
      assert Machine.which_aux_fun(MinimalMachine) == nil
    end

    test "handle_aux/6 dispatches to the 5-arity callback" do
      assert Machine.handle_aux(FullMachine, :leader, :command, :cmd, :aux, :state) ==
               {:reply, :cmd, :aux, :state}
    end

    test "handle_aux/7 dispatches to the 6-arity callback" do
      assert Machine.handle_aux(FullMachine, :leader, :command, :cmd, :aux, :log, :state) ==
               {:no_reply, :aux, :state}
    end
  end

  describe "query/4" do
    test "supports one- and two-arity functions" do
      assert Machine.query(MinimalMachine, &QueryHelpers.by_state/1, :state) == {:state, :state}

      assert Machine.query(MinimalMachine, &QueryHelpers.by_context/2, :state, %{id: 1}) ==
               {:ctx, %{id: 1}, :state}
    end

    test "supports mfa tuples with and without context" do
      assert Machine.query(MinimalMachine, {QueryHelpers, :with_arg, [:a]}, :state) ==
               {:arg, :a, :state}

      assert Machine.query(
               MinimalMachine,
               {QueryHelpers, :with_arg_ctx, [:a], [:with_context]},
               :state,
               %{id: 1}
             ) == {:arg, :a, %{id: 1}, :state}

      assert Machine.query(
               MinimalMachine,
               {QueryHelpers, :with_arg, [:a], []},
               :state,
               %{id: 1}
             ) == {:arg, :a, :state}
    end

    test "unwraps simple machine state" do
      fun = fn cmd, state -> state + cmd end
      simple_state = {:simple, fun, 5}

      assert Machine.query(Simple, &QueryHelpers.by_state/1, simple_state) ==
               {:state, 5}
    end
  end

  describe "Machine.Simple" do
    test "init stores the function and initial state" do
      fun = fn cmd, state -> state + cmd end
      assert {:simple, ^fun, 0} = Simple.init(%{simple_fun: fun, initial_state: 0})
    end

    test "apply runs the function and returns the new state" do
      fun = fn cmd, state -> state + cmd end
      state = {:simple, fun, 1}

      assert {{:simple, ^fun, 3}, 3} = Simple.apply(metadata(), 2, state)
      assert {{:simple, ^fun, 3}, 3, []} = Machine.apply(Simple, metadata(), 2, state)
    end
  end
end
