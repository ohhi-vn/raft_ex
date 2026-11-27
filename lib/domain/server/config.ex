defmodule RaftEx.Domain.Server.Config do
  @moduledoc """
  Immutable per-server configuration record.

  Constructed once during `RaftEx.Domain.Server.init/1` and then stored inside the
  server state map under the `:cfg` key.  Fields are never mutated at runtime
  except for `effective_machine_version`, `effective_machine_module`, and
  `effective_handle_aux_fun`, which are updated when the machine is upgraded.
  """

  @enforce_keys [
    :id,
    :uid,
    :log_id,
    :metrics_key,
    :machine,
    :machine_version,
    :effective_machine_version,
    :effective_machine_module,
    :system_config
  ]

  defstruct [
    :id,
    :uid,
    :log_id,
    :metrics_key,
    :machine,
    :machine_version,
    :effective_machine_version,
    :effective_machine_module,
    :effective_handle_aux_fun,
    :counter,
    :system_config,
    metrics_labels: %{},
    machine_versions: [],
    max_pipeline_count: 4096,
    max_append_entries_rpc_batch_size: 128,
    min_recovery_checkpoint_interval: 0,
    compaction_entry_threshold: 8192,
    compaction_wal_bytes: 134_217_728,
    compaction_interval_ms: 120_000,
    command_plugs: [],
    rpc_plugs: []
  ]

  @type t :: %__MODULE__{
          id: RaftEx.Domain.Types.server_id(),
          uid: RaftEx.Domain.Types.uid(),
          log_id: binary(),
          metrics_key: atom(),
          metrics_labels: map(),
          machine: RaftEx.Domain.Machine.machine(),
          machine_version: RaftEx.Domain.Machine.version(),
          machine_versions: [{RaftEx.Domain.Types.index(), RaftEx.Domain.Machine.version()}],
          effective_machine_version: RaftEx.Domain.Machine.version(),
          effective_machine_module: module(),
          effective_handle_aux_fun: {:handle_aux, 5 | 6} | nil,
          max_pipeline_count: pos_integer(),
          max_append_entries_rpc_batch_size: pos_integer(),
          min_recovery_checkpoint_interval: non_neg_integer(),
          compaction_entry_threshold: pos_integer() | nil,
          compaction_wal_bytes: pos_integer() | nil,
          compaction_interval_ms: pos_integer() | nil,
          counter: term(),
          system_config: map(),
          command_plugs: [term()],
          rpc_plugs: [term()]
        }
end
