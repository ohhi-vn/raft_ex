defmodule RaftEx.Domain.Server.ConfigTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Server.Config

  defp required_fields do
    [
      id: {:server1, node()},
      uid: "uid-123",
      log_id: "log-1",
      metrics_key: :test_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{}
    ]
  end

  describe "struct/1" do
    test "requires the enforced keys" do
      assert_raise ArgumentError, fn -> struct!(Config, []) end
    end

    test "fills defaults for optional fields" do
      config = struct(Config, required_fields())

      assert config.metrics_labels == %{}
      assert config.machine_versions == []
      assert config.max_pipeline_count == 4096
      assert config.max_append_entries_rpc_batch_size == 128
      assert config.min_recovery_checkpoint_interval == 0
      assert config.command_plugs == []
      assert config.rpc_plugs == []
      assert is_nil(config.counter)
      assert is_nil(config.effective_handle_aux_fun)
    end

    test "accepts overrides for optional fields" do
      config =
        struct(
          Config,
          required_fields() ++
            [
              max_pipeline_count: 10,
              command_plugs: [:plug],
              rpc_plugs: [:rpc_plug],
              metrics_labels: %{app: :test},
              machine_versions: [{1, 2}],
              counter: :counter,
              effective_handle_aux_fun: {:handle_aux, 5}
            ]
        )

      assert config.max_pipeline_count == 10
      assert config.command_plugs == [:plug]
      assert config.rpc_plugs == [:rpc_plug]
      assert config.metrics_labels == %{app: :test}
      assert config.machine_versions == [{1, 2}]
      assert config.counter == :counter
      assert config.effective_handle_aux_fun == {:handle_aux, 5}
    end
  end
end
