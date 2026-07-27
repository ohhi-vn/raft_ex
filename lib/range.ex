defmodule RaftEx.Range do
  @type t :: nil | {non_neg_integer(), non_neg_integer()}

  def new(start) when is_integer(start), do: {start, start}

  def new(start, ending)
      when is_integer(start) and is_integer(ending) and start <= ending,
      do: {start, ending}

  def new(_, _), do: nil

  def add(nil, range), do: range
  def add(range, nil), do: range

  def add({add_start, add_end}, {start, ending})
      when start <= add_end + 1 and ending + 1 >= add_start do
    {min(add_start, start), max(add_end, ending)}
  end

  def add(add_range, _range), do: add_range

  def combine(nil, range), do: range
  def combine({add_start, add_end}, {start, _}), do: {min(add_start, start), add_end}
  def combine(add_range, _), do: add_range

  def in?(_, nil), do: false
  def in?(idx, {start, ending}), do: idx >= start and idx <= ending

  def limit(ceil_excl, {start, _}) when is_integer(ceil_excl) and ceil_excl <= start, do: nil

  def limit(ceil_excl, {start, ending})
      when is_integer(ceil_excl) and ceil_excl <= ending,
      do: {start, ceil_excl - 1}

  def limit(ceil_excl, range) when is_integer(ceil_excl), do: range

  def truncate(up_to_incl, {_, ending})
      when is_integer(up_to_incl) and is_integer(ending) and up_to_incl >= ending,
      do: nil

  def truncate(up_to_incl, {start, ending})
      when is_integer(up_to_incl) and is_integer(start) and up_to_incl >= start,
      do: {up_to_incl + 1, ending}

  def truncate(up_to_incl, range) when is_integer(up_to_incl), do: range

  def size(nil), do: 0
  def size({start, ending}), do: ending - start + 1

  def extend(idx, {start, ending}) when idx == ending + 1, do: {start, idx}
  def extend(idx, nil) when is_integer(idx), do: new(idx)
  def extend(idx, range), do: raise("cannot_extend #{inspect(idx)} #{inspect(range)}")

  def overlap({req_start, req_end}, {start, ending}) do
    new(max(req_start, start), min(req_end, ending))
  end

  def overlap(_, _), do: nil

  def subtract(_, nil), do: []
  def subtract(nil, range), do: [range]

  def subtract({_sub_start, _sub_end} = sub_range, {start, ending} = range) do
    case overlap(sub_range, range) do
      nil ->
        [range]

      {o_start, o_end} ->
        [new(start, o_start - 1), new(o_end + 1, ending)]
        |> Enum.reject(&is_nil/1)
    end
  end

  def fold(nil, _fun, acc), do: acc

  def fold({s, e}, fun, acc) do
    Enum.reduce(s..e, acc, fun)
  end
end
