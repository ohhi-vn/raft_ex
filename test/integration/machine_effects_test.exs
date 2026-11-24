defmodule RaftEx.MachineEffectsIntegrationTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Machine

  defmodule EffectMachine do
    @behaviour Machine

    @impl true
    def init(conf), do: conf

    @impl true
    def apply(_meta, cmd, state) do
      effects = [
        {:aux, :machine_effect},
        :garbage_collection,
        {:timer, :tick, 60_000},
        {:send_msg, self(), :hello},
        {:checkpoint, 1, state}
      ]

      {state, {:ok, cmd}, effects}
    end
  end

  setup do
    data_dir = Path.join(System.tmp_dir!(), "raft_effects_#{System.unique_integer([:positive])}")
    File.mkdir_p!(data_dir)

    RaftEx.App.System.stop_default()
    :persistent_term.erase({:"$ra_system", :default})
    RaftEx.start_in(data_dir)

    on_exit(fn ->
      for [name] <- :ets.match(:ra_state, {:"$1", :_, :_}) do
        case Process.whereis(name) do
          nil -> :ok
          pid -> :gen_statem.stop(pid)
        end
      end

      :ets.match_delete(:ra_state, {:_, :_, :_})
      :persistent_term.erase({:"$ra_system", :default})
      File.rm_rf!(data_dir)
    end)

    :ok
  end

  test "machine effects are handled when entries are applied" do
    machine = {:machine, EffectMachine, %{}}
    name = :"effects_cluster_#{System.unique_integer([:positive])}"

    {:ok, [{_sname, _} = sid | _]} =
      RaftEx.form_cluster(name, machine, [node()], timeout: 5_000)

    assert {:ok, _reply, _leader} = RaftEx.process_command(sid, :emit_effects, 3_000)
    Process.sleep(200)
  end
end
