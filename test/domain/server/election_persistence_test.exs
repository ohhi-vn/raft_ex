defmodule RaftEx.Domain.Server.ElectionPersistenceTest do
  use ExUnit.Case, async: false

  alias RaftEx.Domain.Server.{Config, Election}
  alias RaftEx.Domain.Types
  alias RaftEx.Infra.Persistence.Log

  @uid "election-persist-fail-uid"
  @self_id {:server1, node()}
  @peer2 {:server2, node()}

  defp config(meta_name) do
    struct!(Config,
      id: @self_id,
      uid: @uid,
      log_id: "log",
      metrics_key: :election_persist_metrics,
      machine: {:machine, RaftEx.Domain.Machine.Simple, %{}},
      machine_version: 0,
      effective_machine_version: 0,
      effective_machine_module: RaftEx.Domain.Machine.Simple,
      system_config: %{names: %{log_meta: meta_name}}
    )
  end

  defp state(meta_name) do
    %{
      cfg: config(meta_name),
      current_term: 2,
      log: %Log{range: {0, 5}, last_term: 2},
      cluster: %{
        @self_id => %{voter_status: %{membership: :voter}},
        @peer2 => %{voter_status: %{membership: :voter}}
      }
    }
  end

  test "vote denied when term/vote persistence fails" do
    missing = :"missing_meta_#{System.unique_integer([:positive])}"
    # No GenServer and no ETS table under this name, so persistence must fail.
    assert :ets.info(missing) == :undefined

    rpc = %Types.RequestVoteRpc{
      term: 3,
      candidate_id: @peer2,
      last_log_index: 6,
      last_log_term: 3
    }

    assert {false, _state, []} = Election.handle_vote_request(rpc, state(missing))
  end

  test "vote granted when persistence succeeds via real LogMeta" do
    data_dir = RaftEx.TestSupport.tmp_dir("election_persist_ok")
    meta = :"election_persist_#{System.unique_integer([:positive])}"

    start_supervised!(
      {RaftEx.Infra.Persistence.LogMeta,
       %{name: :election_persist, names: %{log_meta: meta}, data_dir: data_dir}}
    )

    rpc = %Types.RequestVoteRpc{
      term: 3,
      candidate_id: @peer2,
      last_log_index: 6,
      last_log_term: 3
    }

    assert {true, new_state, effects} = Election.handle_vote_request(rpc, state(meta))
    assert new_state.current_term == 3
    assert effects == [{:store_meta, :current_term, 3}, {:store_meta, :voted_for, @peer2}]
  end
end
