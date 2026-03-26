defmodule RaftEx.Seq do
  @moduledoc "Sparse sequence representation (ordered high→low)."

  @type t :: [non_neg_integer() | {non_neg_integer(), non_neg_integer()}]

  def append(idx, []) when is_integer(idx), do: [idx]

  def append(idx, [idx_n1, idx_n2 | rest])
      when idx == idx_n1 + 1 and idx == idx_n2 + 2 do
    [{idx_n2, idx} | rest]
  end

  def append(idx, [{idx_n, idx_n1} | rest]) when idx == idx_n1 + 1 do
    [{idx_n, idx} | rest]
  end

  def append(idx, [prev | _] = seq) when is_integer(idx) do
    last = if is_tuple(prev), do: elem(prev, 1), else: prev

    if idx > last do
      [idx | seq]
    else
      seq
    end
  end

  def from_list(list) do
    list |> Enum.sort() |> Enum.uniq() |> Enum.reduce([], &append/2)
  end

  def floor(floor_idx_incl, seq) when is_list(seq) do
    floor0(floor_idx_incl, seq, [])
  end

  def limit(ceil_idx_incl, [last | rest]) when is_integer(last) and last > ceil_idx_incl do
    limit(ceil_idx_incl, rest)
  end

  def limit(ceil_idx_incl, [{_, _} = t | rest]) when is_integer(ceil_idx_incl) do
    case RaftEx.Range.limit(ceil_idx_incl + 1, t) do
      nil ->
        limit(ceil_idx_incl, rest)

      {i, i} ->
        [i | rest]

      {i, i2} when i == i2 - 1 ->
        [i2, i | rest]

      new_range ->
        [new_range | rest]
    end
  end

  def limit(_ceil, seq), do: seq

  def add([], to), do: to
  def add(add, []), do: add

  def add(add, to) do
    fst = first(add)
    fold(&append/2, limit(fst - 1, to), add)
  end

  def fold(fun, acc, seq) do
    Enum.reduce(Enum.reverse(seq), acc, fn
      {_, _} = range, acc -> RaftEx.Range.fold(range, fun, acc)
      idx, acc -> fun.(idx, acc)
    end)
  end

  def expand(seq) do
    fold(fn i, acc -> [i | acc] end, [], seq)
  end

  def first([]), do: nil

  def first(seq) do
    case List.last(seq) do
      {i, _} -> i
      i -> i
    end
  end

  def last([]), do: nil

  def last([head | _]) do
    case head do
      {_, i} -> i
      i -> i
    end
  end

  def length(seq) do
    Enum.reduce(seq, 0, fn
      idx, acc when is_integer(idx) -> acc + 1
      range, acc -> acc + RaftEx.Range.size(range)
    end)
  end

  def in?(_, []), do: false
  def in?(idx, [idx | _]), do: true

  def in?(idx, [next | rest]) when is_integer(next) do
    in?(idx, rest)
  end

  def in?(idx, [range | rest]) do
    if RaftEx.Range.in?(idx, range), do: true, else: in?(idx, rest)
  end

  def range([]), do: nil
  def range(seq), do: RaftEx.Range.new(first(seq), last(seq))

  def in_range(_range, []), do: []
  def in_range(nil, _), do: []

  def in_range({start, ending}, seq) do
    seq |> floor(start) |> limit(ending)
  end

  def has_overlap?(_, []), do: false
  def has_overlap?(nil, _), do: false

  def has_overlap?({start, ending}, seq) do
    has_overlap0(start, ending, seq)
  end

  def remove_prefix(prefix, seq) do
    drop_prefix(next_iter(iterator(prefix)), next_iter(iterator(seq)))
  end

  def iterator(seq) when is_list(seq), do: :lists.reverse(seq)

  def next_iter([]), do: :end_of_seq

  def next_iter([next | rest]) when is_integer(next) do
    {next, rest}
  end

  def next_iter([{next, ending} | rest]) do
    case RaftEx.Range.new(next + 1, ending) do
      nil -> {next, rest}
      next_range -> {next, [next_range | rest]}
    end
  end

  # ---- private helpers -----------------------------------------------

  defp floor0(floor_idx, [last | rest], acc) when is_integer(last) and last >= floor_idx do
    floor0(floor_idx, rest, [last | acc])
  end

  defp floor0(floor_idx, [{_, _} = t | rest], acc) do
    case RaftEx.Range.truncate(floor_idx - 1, t) do
      nil ->
        Enum.reverse(acc)

      {i, i} ->
        floor0(floor_idx, rest, [i | acc])

      {i, i2} when i == i2 - 1 ->
        floor0(floor_idx, rest, [i, i2 | acc])

      new_range ->
        floor0(floor_idx, rest, [new_range | acc])
    end
  end

  defp floor0(_floor_idx, _seq, acc), do: Enum.reverse(acc)

  defp has_overlap0(_start, _ending, []), do: false

  defp has_overlap0(start, ending, [idx | rest]) when is_integer(idx) do
    cond do
      idx > ending -> has_overlap0(start, ending, rest)
      idx >= start -> true
      true -> false
    end
  end

  defp has_overlap0(start, ending, [{r_start, r_end} | rest]) do
    cond do
      r_start > ending -> has_overlap0(start, ending, rest)
      r_end < start -> false
      true -> true
    end
  end

  defp drop_prefix({idx, pi}, {idx, si}) do
    drop_prefix(next_iter(pi), next_iter(si))
  end

  defp drop_prefix(_, :end_of_seq), do: {:ok, []}

  defp drop_prefix(:end_of_seq, {idx, rev_seq}) do
    {:ok, add(Enum.reverse(rev_seq), [idx])}
  end

  defp drop_prefix({pref_idx, pi}, {idx, _si} = i) when pref_idx < idx do
    drop_prefix(next_iter(pi), i)
  end

  defp drop_prefix({pref_idx, _pi}, {idx, _si}) when idx < pref_idx do
    {:error, :not_prefix}
  end
end
