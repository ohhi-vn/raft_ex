defmodule RaftEx.Domain.PlugTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.{Conn, Plug}

  defmodule EchoPlug do
    @behaviour Plug

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, opts), do: Conn.assign(Conn.put_resp(conn, :echo), :opts, opts)
  end

  describe "init/1" do
    test "returns the options unchanged" do
      assert Plug.init(foo: :bar) == [foo: :bar]
      assert Plug.init(nil) == nil
      assert Plug.init(%{a: 1}) == %{a: 1}
    end
  end

  describe "call/3" do
    test "invokes the plug module with the conn and initialized opts" do
      conn = Plug.call(EchoPlug, %Conn{}, value: 7)

      assert conn.resp == :echo
      assert Conn.get_assign(conn, :opts) == [value: 7]
    end

    test "works with an empty conn and nil opts" do
      conn = Plug.call(EchoPlug, %Conn{type: :command}, nil)

      assert Conn.get_assign(conn, :opts) == nil
      assert conn.type == :command
    end

    test "requires an atom module" do
      assert_raise FunctionClauseError, fn -> Plug.call("EchoPlug", %Conn{}, []) end
    end
  end

  describe "behaviour" do
    test "a conforming module implements init/1 and call/2" do
      assert function_exported?(EchoPlug, :init, 1)
      assert function_exported?(EchoPlug, :call, 2)
      assert EchoPlug.init(:opts) == :opts
    end
  end
end
