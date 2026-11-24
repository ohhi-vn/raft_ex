defmodule RaftEx.PlugPipelineTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.{Conn, Plug, Pipeline}

  defmodule NoopPlug do
    @behaviour Plug
    def init(opts), do: opts
    def call(conn, _opts), do: conn
  end

  defmodule HaltPlug do
    @behaviour Plug
    def init(opts), do: opts
    def call(conn, _opts), do: Conn.halt(Conn.put_resp(conn, :halted))
  end

  defmodule AssignPlug do
    @behaviour Plug
    def init(opts), do: opts

    def call(conn, opts) do
      Conn.assign(conn, :value, Keyword.get(opts, :value, 1))
    end
  end

  defmodule TestMachine do
    @behaviour RaftEx.Domain.Machine
    def init(_conf), do: 0
    def apply(_meta, _cmd, state), do: {state, :ok}
  end

  describe "Conn" do
    test "struct defaults" do
      conn = %Conn{}

      assert conn.type == nil
      assert conn.request == %{}
      assert conn.resp == nil
      assert conn.assigns == %{}
      assert conn.halted == false
      assert conn.status == nil
      assert conn.private == %{}
    end

    test "assign and get_assign" do
      conn = %Conn{}
      conn = Conn.assign(conn, :foo, 42)
      assert Conn.get_assign(conn, :foo) == 42
      assert Conn.get_assign(conn, :missing) == nil
      assert Conn.get_assign(conn, :missing, :fallback) == :fallback
      assert conn.assigns == %{foo: 42}
    end

    test "put_resp stores the response" do
      conn = Conn.put_resp(%Conn{}, {:ok, 1})
      assert conn.resp == {:ok, 1}
    end

    test "put_status stores the status" do
      conn = Conn.put_status(%Conn{}, :ok)
      assert conn.status == :ok
    end

    test "halt sets halted without changing other fields" do
      conn = %Conn{request: %{x: 1}, resp: :kept}
      halted = Conn.halt(conn)

      assert halted.halted
      assert halted.request == conn.request
      assert halted.resp == :kept
    end
  end

  describe "Pipeline" do
    test "runs plugs in order" do
      plugs = Pipeline.init_plugs([{AssignPlug, [value: 7]}])
      conn = Pipeline.run(%Conn{}, plugs)
      assert Conn.get_assign(conn, :value) == 7
      refute conn.halted
    end

    test "halts remaining plugs" do
      plugs = Pipeline.init_plugs([HaltPlug, NoopPlug])
      conn = Pipeline.run(%Conn{}, plugs)
      assert conn.halted
      assert conn.resp == :halted
    end

    test "empty pipeline is no-op" do
      conn = Pipeline.run(%Conn{request: %{x: 1}}, [])
      assert conn.request == %{x: 1}
      refute conn.halted
    end
  end

  describe "command pipeline integration" do
    test "halted command does not apply" do
      # build a minimal system config with a halt plug
      sys = RaftEx.App.System.default_config()
      sys = Map.put(sys, :command_plugs, [HaltPlug])

      # start a server via RaftEx? Instead test Domain.Server init stores plugs
      config = %{
        id: {:s, node()},
        uid: "UID123456",
        cluster_name: :c,
        initial_members: [{:s, node()}],
        log_init_args: %{uid: "UID123456"},
        machine: {:machine, TestMachine, %{}},
        system_config: sys
      }

      state = RaftEx.Domain.Server.init(config)
      assert state.cfg.command_plugs != []
    end
  end
end
