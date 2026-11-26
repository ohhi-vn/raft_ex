defmodule RaftEx.TestSupport.Linearizability do
  @moduledoc """
  In-process linearizability checker for RaftEx chaos tests.

  Records a history of register operations (write/read) and verifies that
  every read returns a value consistent with a sequential execution: a read
  must observe either the initial value or the value of some write that
  completed before it, and writes take effect in invocation order.
  """

  defmodule History do
    @moduledoc "Recorded operation history for a single register."

    defstruct ops: [], initial: nil

    @type op :: %{
            id: term(),
            type: :read | :write,
            value: term(),
            invoke_ts: integer(),
            ok_ts: integer() | nil,
            result: term() | nil,
            status: :ok | :fail | :pending
          }

    @type t :: %__MODULE__{ops: [op()], initial: term()}
  end

  @doc "Start a new history with the given initial value."
  def new(initial \\ nil), do: %History{ops: [], initial: initial}

  @doc "Record an invoked operation (returns updated history and op id)."
  def invoke(history, type, value) do
    id = System.unique_integer([:positive])
    ts = System.monotonic_time(:microsecond)

    op = %{
      id: id,
      type: type,
      value: value,
      invoke_ts: ts,
      ok_ts: nil,
      result: nil,
      status: :pending
    }

    {%{history | ops: history.ops ++ [op]}, id}
  end

  @doc "Mark an operation complete with its result."
  def complete(history, id, result, status \\ :ok) do
    ops =
      Enum.map(history.ops, fn op ->
        if op.id == id do
          %{op | ok_ts: System.monotonic_time(:microsecond), result: result, status: status}
        else
          op
        end
      end)

    %{history | ops: ops}
  end

  @doc """
  Verify the history is linearizable (sequential consistency for a register).
  Returns :ok or {:error, reason}.
  """
  def verify(%History{} = history) do
    completed = Enum.filter(history.ops, &(&1.status == :ok and &1.ok_ts != nil))
    # Sort by completion time.
    ordered = Enum.sort_by(completed, & &1.ok_ts)

    # Walk operations; track the last written value visible to reads.
    {_, result} =
      Enum.reduce_while(ordered, {history.initial, :ok}, fn op, {last_write, _} ->
        handle_op(op, last_write)
      end)

    result
  end

  defp handle_op(%{type: :write, value: value}, _last_write) do
    {:cont, {value, :ok}}
  end

  defp handle_op(%{type: :read, result: result, id: id}, last_write) do
    if result == last_write do
      {:cont, {last_write, :ok}}
    else
      {:halt, {last_write, {:error, {:stale_read, id, result, last_write}}}}
    end
  end

  @doc "Dump history to a file for replay/debugging."
  def dump(history, name) do
    path =
      Path.join(System.tmp_dir!(), "history_#{name}_#{System.system_time(:millisecond)}.term")

    File.write!(path, :erlang.term_to_binary(history))
    path
  end
end
