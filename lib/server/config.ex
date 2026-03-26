defmodule RaftEx.Server.Config do
  @moduledoc """
  Immutable per-server configuration record.

  Constructed once during `RaftEx.Server.init/1` and then stored inside the
  server state map under the `:cfg` key.  Fields are never mutated at runtime
  except for `effective_machine_version`, `effective_machine_module`, and
  `effective_handle_aux_fun`, which are updated when the machine is upgraded.
  """

  @enforce_keys [:id, :uid, :log_id, :metrics_key, :machine, :machine_version,
                 :effective_machine_version, :effective_machine_module, :system_config]

  defstruct [
    # Identity
    :id,
    :uid,
    :log_id,
    :metrics_key,
    metrics_labels: %{},

    # Machine
    :machine,
    :machine_version,
    machine_versions: [],
    :effective_machine_version,
    :effective_machine_module,
    :effective_handle_aux_fun,

    # Tuning
    max_pipeline_count: 4096,
    max_append_entries_rpc_batch_size: 128,
    min_recovery_checkpoint_interval: 0,

    # Infrastructure
    :counter,
    :system_config
  ]

  @type t :: %__MODULE__{
          id: RaftEx.Types.server_id(),
          uid: RaftEx.Types.uid(),
          log_id: binary(),
          metrics_key: atom(),
          metrics_labels: map(),
          machine: RaftEx.Machine.machine(),
          machine_version: RaftEx.Machine.version(),
          machine_versions: [{RaftEx.Types.index(), RaftEx.Machine.version()}],
          effective_machine_version: RaftEx.Machine.version(),
          effective_machine_module: module(),
          effective_handle_aux_fun: {:handle_aux, 5 | 6} | nil,
          max_pipeline_count: pos_integer(),
          max_append_entries_rpc_batch_size: pos_integer(),
          min_recovery_checkpoint_interval: non_neg_integer(),
          counter: term(),
          system_config: map()
        }
end
