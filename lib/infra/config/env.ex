defmodule RaftEx.Infra.Config.Env do
  @moduledoc """
  Resolves RaftEx runtime configuration from the application environment.

  In particular, it computes the per-node data directory and configures the
  logger module used by the underlying `ra` library.
  """

  def data_dir do
    base =
      case Application.get_env(:raft_ex, :data_dir) do
        nil ->
          case Application.get_env(:ra, :data_dir) do
            nil ->
              {:ok, cwd} = File.cwd()
              cwd

            dir ->
              dir
          end

        dir ->
          dir
      end

    node_str = node() |> Atom.to_string()
    Path.join(base, node_str)
  end

  def server_data_dir(system, uid) when is_atom(system) do
    %{data_dir: dir} = RaftEx.App.System.fetch(system)
    me = if is_binary(uid), do: uid, else: to_string(uid)
    Path.join(dir, me)
  end

  def configure_logger(module) do
    :persistent_term.put(:"$ra_logger", module)
  end
end
