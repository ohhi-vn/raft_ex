defmodule RaftEx.Domain.Server.LocalRoleTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server
  alias RaftEx.Domain.Server.{Cluster, Config}
  alias RaftEx.Infra.Persistence.Log

  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defp config(uid \\ "role-uid") do
    struct!(Config,
      id: @self_id,
      uid: uid,
      log_id: "role-log",
      metrics_key: :role_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{}}
    )
  end

  defp state(cluster, uid \\ "role-uid") do
    %{
      cfg: config(uid),
      current_term: 2,
      # Stale local field that must be ignored in favor of the cluster map.
      membership: :voter,
      cluster: cluster,
      log: %Log{entries: %{}, range: nil, last_term: 0, next_index: 1}
    }
  end

  test "non-voter cluster entry is reported even with a stale local field" do
    cluster = %{@self_id => %{voter_status: %{membership: :non_voter, uid: nil}}}

    assert Server.get_membership(state(cluster)) == :non_voter
  end

  test "promoted member reports voter" do
    cluster = %{@self_id => %{voter_status: %{membership: :voter, uid: "real-uid"}}}

    assert Server.get_membership(state(cluster, "real-uid")) == :voter
  end

  test "demoted member reports non-voter" do
    cluster = %{@self_id => %{voter_status: %{membership: :non_voter, uid: "real-uid"}}}

    assert Server.get_membership(state(cluster, "real-uid")) == :non_voter
  end

  test "cluster lookup ignores uid mismatch" do
    cluster = %{@self_id => %{voter_status: %{membership: :non_voter, uid: nil}}}

    assert Cluster.get_membership(cluster, @self_id, "different-uid", :voter) == :non_voter
  end
end
