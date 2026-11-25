defmodule RaftEx.Domain.PipelineTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.{Conn, Pipeline}

  defmodule AssignPlug do
    def init(opts), do: Keyword.put_new(opts, :value, 1)
    def call(conn, opts), do: Conn.assign(conn, :value, opts[:value])
  end

  defmodule HaltPlug do
    def init(opts), do: opts
    def call(conn, _opts), do: Conn.halt(conn)
  end

  defmodule CallablePlug do
    defstruct []

    def call(conn, _opts), do: Conn.assign(conn, :callable, true)
  end

  describe "init_plugs/1" do
    test "initializes module plugs with default and explicit opts" do
      assert Pipeline.init_plugs([AssignPlug]) == [{AssignPlug, [value: 1]}]
      assert Pipeline.init_plugs([{AssignPlug, [value: 9]}]) == [{AssignPlug, [value: 9]}]
    end

    test "passes functions through unchanged" do
      fun = fn conn -> conn end
      fun2 = fn conn, _opts -> conn end

      assert Pipeline.init_plugs([fun, {fun2, [a: 1]}]) == [fun, {fun2, [a: 1]}]
    end

    test "passes unknown plug terms through unchanged" do
      assert Pipeline.init_plugs([%CallablePlug{}]) == [%CallablePlug{}]
      assert Pipeline.init_plugs([]) == []
    end
  end

  describe "run/2" do
    test "runs module plugs in order" do
      plugs = Pipeline.init_plugs([{AssignPlug, [value: 7]}])
      conn = Pipeline.run(%Conn{}, plugs)

      assert Conn.get_assign(conn, :value) == 7
      refute conn.halted
    end

    test "runs function plugs" do
      fun = fn conn -> Conn.assign(conn, :fun, 1) end
      fun2 = fn conn, opts -> Conn.assign(conn, :fun2, opts[:n]) end

      conn = Pipeline.run(%Conn{}, [fun, {fun2, [n: 2]}])

      assert Conn.get_assign(conn, :fun) == 1
      assert Conn.get_assign(conn, :fun2) == 2
    end

    test "halts remaining plugs after a halt" do
      plugs = Pipeline.init_plugs([{AssignPlug, [value: 1]}, HaltPlug, {AssignPlug, [value: 2]}])
      conn = Pipeline.run(%Conn{}, plugs)

      assert conn.halted
      assert Conn.get_assign(conn, :value) == 1
    end

    test "does not run anything when the conn starts halted" do
      conn = Pipeline.run(%Conn{halted: true}, Pipeline.init_plugs([AssignPlug]))
      refute Conn.get_assign(conn, :value)
    end

    test "an empty pipeline is a no-op" do
      conn = %Conn{request: %{x: 1}}
      assert Pipeline.run(conn, []) == conn
    end

    test "runs a bare module plug with empty options" do
      conn = Pipeline.run(%Conn{}, [CallablePlug])

      assert Conn.get_assign(conn, :callable) == true
      refute conn.halted
    end
  end
end
