defmodule RaftEx.Infra.ProcessTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Process.{MachineEts, Worker}

  describe "Worker" do
    test "runs queued funs and mfas" do
      parent = self()
      {:ok, pid} = Worker.start_link(%{id: :worker_test, friendly_name: "test"})
      ref = make_ref()

      assert :ok = Worker.queue_work(pid, fn -> send(parent, {:done, ref}) end, fn _ -> :ok end)
      assert_receive {:done, ^ref}

      assert :ok =
               Worker.queue_work(pid, {self(), :send, [{:done, make_ref()}]}, fn _ -> :ok end)
    end

    test "reports errors through the error fun" do
      parent = self()
      {:ok, pid} = Worker.start_link(%{id: :error_worker})
      ref = make_ref()

      assert :ok =
               Worker.queue_work(
                 pid,
                 fn -> raise "boom" end,
                 fn {:error, %RuntimeError{}} -> send(parent, {:error_reported, ref}) end
               )

      assert_receive {:error_reported, ^ref}
    end

    test "ignores unknown info messages" do
      {:ok, pid} = Worker.start_link(%{id: :info_worker})
      assert {:noreply, state} = Worker.handle_info(:unknown, %{log_id: "test"})
      assert state.log_id == "test"
      assert Process.alive?(pid)
    end
  end

  describe "MachineEts" do
    test "creates ETS tables via call and cast" do
      assert {:ok, state} = MachineEts.init([])
      table = :"machine_ets_test_#{System.unique_integer([:positive])}"

      assert {:reply, :ok, new_state} =
               MachineEts.handle_call(
                 {:new_ets, table, [:set, :public, :named_table]},
                 nil,
                 state
               )

      assert :ets.info(table) != :undefined
      assert Map.has_key?(new_state, table)

      table2 = :"machine_ets_cast_#{System.unique_integer([:positive])}"

      assert {:noreply, new_state} =
               MachineEts.handle_cast(
                 {:new_ets, table2, [:set, :public, :named_table]},
                 new_state
               )

      assert :ets.info(table2) != :undefined
    end

    test "create_table delegates to the registered server" do
      case MachineEts.start_link([]) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end

      table = :"machine_ets_create_#{System.unique_integer([:positive])}"
      assert :ok = MachineEts.create_table(table, [:set, :public, :named_table])
      assert :ets.info(table) != :undefined
    end
  end
end
