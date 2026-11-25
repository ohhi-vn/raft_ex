defmodule RaftEx.Domain.Pipeline do
  @moduledoc """
  Runner for Plug-style pipelines.
  """

  alias RaftEx.Domain.Conn

  @type plug :: module() | {module(), term()} | (Conn.t() -> Conn.t()) | {function(), term()}

  @doc "Initialize a list of raw plugs into an executable form."
  @spec init_plugs(list()) :: list()
  def init_plugs(plugs) when is_list(plugs) do
    Enum.map(plugs, &init_plug/1)
  end

  defp init_plug(mod) when is_atom(mod) do
    opts = mod.init([])
    {mod, opts}
  end

  defp init_plug({mod, opts}) when is_atom(mod) do
    {mod, mod.init(opts)}
  end

  defp init_plug(fun) when is_function(fun, 1) do
    fun
  end

  defp init_plug({fun, opts}) when is_function(fun, 2) do
    {fun, opts}
  end

  defp init_plug(other), do: other

  @doc "Run an initialized pipeline over a conn."
  @spec run(Conn.t(), list()) :: Conn.t()
  def run(conn, plugs) when is_list(plugs) do
    Enum.reduce_while(plugs, conn, fn plug, acc ->
      if acc.halted do
        {:halt, acc}
      else
        continue_or_halt(call_plug(plug, acc))
      end
    end)
  end

  defp continue_or_halt(%{halted: true} = conn), do: {:halt, conn}
  defp continue_or_halt(conn), do: {:cont, conn}

  defp call_plug({mod, opts}, conn) when is_atom(mod) do
    mod.call(conn, opts)
  end

  defp call_plug(fun, conn) when is_function(fun, 1) do
    fun.(conn)
  end

  defp call_plug({fun, opts}, conn) when is_function(fun, 2) do
    fun.(conn, opts)
  end

  defp call_plug(plug, conn), do: plug.call(conn, [])
end
