defmodule RaftEx.LruCache do
  @max_size 5

  defstruct max_size: @max_size, items: [], handler: nil

  def new(max_size, handler) do
    %__MODULE__{max_size: max_size, handler: handler}
  end

  def fetch(key, %{items: [{key, value} | _]} = state) do
    {:ok, value, state}
  end

  def fetch(key, %{items: items} = state) do
    case List.keytake(items, key, 0) do
      {value_tuple, rest} ->
        {:ok, elem(value_tuple, 1), %{state | items: [value_tuple | rest]}}

      nil ->
        :error
    end
  end

  def insert(key, value, %{items: items, max_size: m, handler: handler} = state)
      when length(items) == m do
    [old | rest] = Enum.reverse(items)
    if handler, do: handler.(old)
    %{state | items: [{key, value} | Enum.reverse(rest)]}
  end

  def insert(key, value, %{items: items} = state) do
    %{state | items: [{key, value} | items]}
  end

  def evict(key, %{items: items, handler: handler} = state) do
    case List.keytake(items, key, 0) do
      {evicted, rest} ->
        if handler, do: handler.(evicted)
        {evicted, %{state | items: rest}}

      nil ->
        :error
    end
  end

  def evict_all(%{items: items, handler: handler} = state) do
    if handler, do: Enum.each(items, handler)
    %{state | items: []}
  end

  def size(%{items: items}), do: length(items)
  def max_size(%{max_size: ms}), do: ms
end
