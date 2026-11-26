defmodule RaftEx.Domain.Server.RecoveryIdempotencyTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Machine
  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.Config
  alias RaftEx.Infra.Persistence.Log

  @self_id {:server_cov, :nonode@nohost}

  setup do
    meta_name = :"idemp_meta_#{System.unique_integer([:positive])}"
    Process.put(:idemp_meta_name, meta_name)
    :ok
  end

  defp config do
    struct!(Config,
      id: @self_id,
      uid: "idempotency-uid",
      log_id: "idempotency-log",
      metrics_key: :idempotency_metrics,
      machine: {:machine, Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: Machine.Simple,
      effective_handle_aux_fun: nil,
      command_plugs: [],
      rpc_plugs: [],
      machine_versions: [{0, 0}],
      counter: nil,
      max_pipeline_count: 4096,
      system_config: %{names: %{log_meta: Process.get(:idemp_meta_name)}}
    )
  end

  defp log(entries) do
    entries_map = Map.new(entries, fn {idx, term, cmd} -> {idx, {idx, term, cmd}} end)
    indexes = Enum.map(entries, &elem(&1, 0))

    {range, last_term} =
      case indexes do
        [] -> {nil, 0}
        _ -> {{Enum.min(indexes), Enum.max(indexes)}, entries |> List.last() |> elem(1)}
      end

    last_index = if indexes == [], do: 0, else: Enum.max(indexes)

    %Log{
      entries: entries_map,
      range: range,
      last_term: last_term,
      next_index: last_index + 1,
      last_written_index_term: {last_index, last_term},
      current_snapshot: nil,
      meta_name: Process.get(:idemp_meta_name),
      cfg: %{uid: "idempotency-uid"}
    }
  end

  defp state do
    %{
      cfg: config(),
      current_term: 2,
      commit_index: 1,
      last_applied: 1,
      persisted_last_applied: 1,
      leader_id: nil,
      voted_for: nil,
      commit_latency: 0,
      rpc_from: nil,
      membership: :voter,
      cluster_change_permitted: false,
      cluster_index_term: {0, 0},
      pending_consistent_queries: [],
      pending_release_cursor: nil,
      query_index: 0,
      queries_waiting_heartbeats: :queue.new(),
      cluster: %{},
      log: log([{1, 2, :cmd}, {2, 2, :cmd}]),
      machine_state: :ms,
      aux_state: nil
    }
  end

  test "recover/1 is idempotent" do
    s = state()
    r1 = Server.recover(s)
    r2 = Server.recover(r1)
    assert r1.commit_index == r2.commit_index
    assert r1.last_applied == r2.last_applied
  end
end
