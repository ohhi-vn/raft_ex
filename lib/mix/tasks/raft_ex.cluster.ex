defmodule Mix.Tasks.RaftEx.Cluster do
  @moduledoc """
  Manage RaftEx distributed clusters.

  ## Usage

      mix raft_ex.cluster status            # Show cluster status
      mix raft_ex.cluster connect <nodes>   # Connect to seed nodes
      mix raft_ex.cluster leader <cluster>  # Find cluster leader

  ## Options

    * `--cookie` - Erlang distribution cookie (default: `raft_ex`)
    * `--timeout` - operation timeout in ms (default: 5000)
  """

  use Mix.Task

  @shortdoc "Manage RaftEx distributed clusters"
  @impl Mix.Task
  def run(args) do
    {opts, positional, _invalid} =
      OptionParser.parse(args, strict: [cookie: :string, timeout: :integer])

    cookie = opts[:cookie] || "raft_ex"
    timeout = opts[:timeout] || 5000

    case positional do
      ["status"] ->
        cmd_status(cookie)

      ["connect" | nodes] when nodes != [] ->
        cmd_connect(nodes, cookie)

      ["leader", cluster_name] ->
        cmd_leader(cluster_name, cookie, timeout)

      _ ->
        print_usage()
    end
  end

  defp cmd_status(_cookie) do
    ensure_started()

    case RaftEx.cluster_status() do
      {:ok, %{servers: servers, node: current_node}} ->
        Mix.shell().info("RaftEx Cluster Status")
        Mix.shell().info("  Node: #{inspect(current_node)}")
        Mix.shell().info("  Servers:")

        for s <- servers do
          Mix.shell().info("    #{inspect(s.server)}")
          Mix.shell().info("      State:      #{s.state}")
          Mix.shell().info("      Membership: #{s.membership}")
          Mix.shell().info("      Cluster:    #{inspect(s.cluster)}")
          Mix.shell().info("")
        end

      {:error, :system_not_started} ->
        Mix.shell().error("RaftEx system not started. Run `mix raft_ex.cluster connect` first.")

      {:error, reason} ->
        Mix.shell().error("Failed to get status: #{inspect(reason)}")
    end
  end

  defp cmd_connect(nodes, cookie) do
    ensure_started()

    node_names =
      Enum.map(nodes, fn n ->
        case String.split(n, "@") do
          [_name, _host] -> String.to_atom(n)
          [name] -> String.to_atom("#{name}@#{hostname()}")
        end
      end)

    RaftEx.start_distribution(cookie: String.to_atom(cookie))

    connected = RaftEx.Distribution.connect_to_seeds(node_names, String.to_atom(cookie))

    if connected == [] do
      Mix.shell().warn("No seed nodes connected")
    else
      Mix.shell().info("Connected to: #{inspect(connected)}")
    end

    Mix.shell().info("Cluster nodes: #{inspect(RaftEx.Distribution.cluster_nodes())}")
  end

  defp cmd_leader(cluster_name, _cookie, _timeout) do
    ensure_started()

    case RaftEx.find_leader(String.to_atom(cluster_name)) do
      {:ok, leader} ->
        Mix.shell().info("Leader for #{cluster_name}: #{inspect(leader)}")

      {:error, reason} ->
        Mix.shell().error("Could not find leader: #{inspect(reason)}")
    end
  end

  defp print_usage do
    Mix.shell().info(@moduledoc)
  end

  defp ensure_started do
    case Application.ensure_all_started(:raft_ex) do
      {:ok, _} -> :ok
      {:error, _} -> {:ok, _} = Application.ensure_all_started(:raft_ex)
    end
  end

  defp hostname do
    {:ok, host} = :inet.gethostname()
    List.to_string(host)
  end
end
