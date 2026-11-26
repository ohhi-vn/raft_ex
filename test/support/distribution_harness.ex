defmodule RaftEx.TestSupport.DistributionHarness do
  @moduledoc """
  Shared helpers for network tests that boot real BEAM nodes.

  The suite can run either with no distribution, with `--sname` (short names)
  or with `--name` (long names). The helpers here adapt peer names to the
  coordinator's mode and never change the cookie of an already running node:
  `:peer.start/1` launches peers with the coordinator's original cookie, so
  calling `Node.set_cookie/1` later makes every later peer handshake fail.
  """

  @doc "Ensures distribution is running without disturbing an existing node."
  def ensure_distribution do
    if Node.alive?() do
      :ok
    else
      unique = System.unique_integer([:positive])

      case Node.start(:"raft_ex_dist_#{unique}@127.0.0.1") do
        {:ok, _pid} -> Node.set_cookie(:raft_ex)
        {:error, _reason} -> :ok
      end
    end
  end

  @doc "The cookie peers must use to connect to this node."
  def cookie, do: Node.get_cookie()

  @doc "Boots a hidden-or-visible BEAM node in a subprocess and waits for READY."
  def start_peer(prefix, opts \\ []) do
    unique = System.unique_integer([:positive])
    {name, name_flag} = peer_name(prefix, unique)
    ctl = :"#{prefix}_ctl_#{unique}"

    args = [
      name_flag,
      Atom.to_string(name),
      "--cookie",
      Atom.to_string(cookie())
    ]

    args = if Keyword.get(opts, :hidden, false), do: args ++ ["--hidden"], else: args

    args =
      args ++
        [
          "-e",
          "Process.register(self(), #{inspect(ctl)}); IO.puts(\"READY\"); " <>
            "receive do :stop -> :ok after 30_000 -> :ok end"
        ]

    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        :hide,
        args: args
      ])

    await_ready(port)
    %{name: name, ctl: ctl, port: port}
  end

  @doc "Stops a peer started with start_peer/2."
  def stop_peer(%{name: name, ctl: ctl, port: port}) do
    if Node.alive?() do
      send({ctl, name}, :stop)
      Node.disconnect(name)
    end

    try do
      Port.close(port)
    rescue
      _ -> :ok
    end
  end

  defp peer_name(prefix, unique) do
    base = "#{prefix}_#{unique}"
    host = host_of(node())

    if Node.alive?() and not String.contains?(host, ".") do
      {String.to_atom("#{base}@#{host}"), "--sname"}
    else
      {String.to_atom("#{base}@127.0.0.1"), "--name"}
    end
  end

  defp host_of(node_name) do
    node_name
    |> Atom.to_string()
    |> String.split("@", parts: 2)
    |> List.last()
  end

  defp await_ready(port, acc \\ "") do
    receive do
      {^port, {:data, data}} ->
        acc = acc <> data

        if String.contains?(acc, "READY") do
          :ok
        else
          await_ready(port, acc)
        end

      {^port, {:exit_status, status}} ->
        raise "distribution peer exited before ready (status #{status}): #{acc}"
    after
      15_000 -> raise "distribution peer did not become ready: #{acc}"
    end
  end
end
