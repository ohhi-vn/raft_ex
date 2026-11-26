defmodule RaftExBench.JSON do
  @moduledoc false

  # Minimal JSON encoder/decoder used by the benchmark harness so the suite
  # does not need a runtime dependency. Supports the subset the harness emits:
  # maps with atom or string keys, lists, strings, numbers, booleans, and nil.

  @spec encode(term()) :: binary()
  def encode(term), do: term |> encode_value() |> IO.iodata_to_binary()

  defp encode_value(nil), do: "null"
  defp encode_value(true), do: "true"
  defp encode_value(false), do: "false"
  defp encode_value(n) when is_integer(n), do: Integer.to_string(n)

  defp encode_value(n) when is_float(n) do
    :erlang.float_to_binary(n, [:short])
  end

  defp encode_value(s) when is_binary(s), do: [?", escape(s), ?"]
  defp encode_value(a) when is_atom(a), do: encode_value(Atom.to_string(a))

  defp encode_value(list) when is_list(list) do
    [?[, list |> Enum.map(&encode_value/1) |> Enum.intersperse(?,), ?]]
  end

  defp encode_value(map) when is_map(map) do
    pairs =
      map
      |> Enum.map(fn {k, v} -> {key_string(k), v} end)
      |> Enum.sort_by(fn {k, _} -> k end)

    encoded =
      Enum.map(pairs, fn {k, v} -> [encode_value(k), ?:, encode_value(v)] end)

    [?{, Enum.intersperse(encoded, ?,), ?}]
  end

  defp key_string(k) when is_binary(k), do: k
  defp key_string(k) when is_atom(k), do: Atom.to_string(k)
  defp key_string(k), do: to_string(k)

  defp escape(bin), do: escape(bin, [])

  defp escape(<<>>, acc), do: Enum.reverse(acc)
  defp escape(<<?", rest::binary>>, acc), do: escape(rest, ["\\\"" | acc])
  defp escape(<<?\\, rest::binary>>, acc), do: escape(rest, ["\\\\" | acc])
  defp escape(<<?\n, rest::binary>>, acc), do: escape(rest, ["\\n" | acc])
  defp escape(<<?\r, rest::binary>>, acc), do: escape(rest, ["\\r" | acc])
  defp escape(<<?\t, rest::binary>>, acc), do: escape(rest, ["\\t" | acc])

  defp escape(<<c, rest::binary>>, acc) when c < 0x20 do
    hex = c |> Integer.to_string(16) |> String.pad_leading(4, "0")
    escape(rest, ["\\u" <> hex | acc])
  end

  defp escape(<<c, rest::binary>>, acc), do: escape(rest, [<<c>> | acc])

  @spec decode(binary()) :: term()
  def decode(bin) when is_binary(bin) do
    {value, rest} = bin |> skip_ws() |> parse_value()

    case skip_ws(rest) do
      "" -> value
      other -> raise ArgumentError, "trailing JSON data: #{inspect(other)}"
    end
  end

  defp skip_ws(<<c, rest::binary>>) when c in [?\s, ?\t, ?\n, ?\r], do: skip_ws(rest)
  defp skip_ws(bin), do: bin

  defp parse_value(<<"null", rest::binary>>), do: {nil, rest}
  defp parse_value(<<"true", rest::binary>>), do: {true, rest}
  defp parse_value(<<"false", rest::binary>>), do: {false, rest}
  defp parse_value(<<?", rest::binary>>), do: parse_string(rest, [])
  defp parse_value(<<?[, rest::binary>>), do: parse_array(skip_ws(rest), [])
  defp parse_value(<<?{, rest::binary>>), do: parse_object(skip_ws(rest), %{})

  defp parse_value(bin) do
    {number, rest} = take_number(bin, [])
    {parse_number(number), rest}
  end

  defp parse_string(<<?", rest::binary>>, acc) do
    {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}
  end

  defp parse_string(<<?\\, ?", rest::binary>>, acc), do: parse_string(rest, [?" | acc])
  defp parse_string(<<?\\, ?\\, rest::binary>>, acc), do: parse_string(rest, [?\\ | acc])
  defp parse_string(<<?\\, ?n, rest::binary>>, acc), do: parse_string(rest, [?\n | acc])
  defp parse_string(<<?\\, ?r, rest::binary>>, acc), do: parse_string(rest, [?\r | acc])
  defp parse_string(<<?\\, ?t, rest::binary>>, acc), do: parse_string(rest, [?\t | acc])

  defp parse_string(<<?\\, ?u, hex::binary-size(4), rest::binary>>, acc) do
    codepoint = String.to_integer(hex, 16)
    parse_string(rest, [<<codepoint::utf8>> | acc])
  end

  defp parse_string(<<c, rest::binary>>, acc), do: parse_string(rest, [c | acc])

  defp parse_array(<<?], rest::binary>>, acc) do
    {Enum.reverse(acc), rest}
  end

  defp parse_array(bin, acc) do
    {value, rest} = parse_value(bin)

    case skip_ws(rest) do
      <<?,, rest2::binary>> -> parse_array(skip_ws(rest2), [value | acc])
      <<?], rest2::binary>> -> {Enum.reverse([value | acc]), rest2}
      other -> raise ArgumentError, "invalid JSON array near #{inspect(other)}"
    end
  end

  defp parse_object(<<?}, rest::binary>>, acc), do: {acc, rest}

  defp parse_object(bin, acc) do
    {key, rest} = parse_value(bin)
    true = is_binary(key)

    case skip_ws(rest) do
      <<?:, rest2::binary>> ->
        {value, rest3} = parse_value(skip_ws(rest2))

        case skip_ws(rest3) do
          <<?,, rest4::binary>> -> parse_object(skip_ws(rest4), Map.put(acc, key, value))
          <<?}, rest4::binary>> -> {Map.put(acc, key, value), rest4}
          other -> raise ArgumentError, "invalid JSON object near #{inspect(other)}"
        end

      other ->
        raise ArgumentError, "expected ':' near #{inspect(other)}"
    end
  end

  defp take_number(<<c, rest::binary>>, acc) when c in ~c"0123456789+-.eE" do
    take_number(rest, [c | acc])
  end

  defp take_number(rest, acc), do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp parse_number(number) do
    if String.contains?(number, [".", "e", "E"]) do
      String.to_float(number)
    else
      String.to_integer(number)
    end
  end
end
