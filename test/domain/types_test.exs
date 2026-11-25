defmodule RaftEx.Domain.TypesTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Types

  describe "RPC structs" do
    test "AppendEntriesRpc defaults and pattern matching" do
      rpc = %Types.AppendEntriesRpc{term: 5, leader_id: {:s1, :node@host}, prev_log_index: 9}

      assert rpc.term == 5
      assert rpc.leader_id == {:s1, :node@host}
      assert rpc.prev_log_index == 9
      assert rpc.entries == []
      assert is_nil(rpc.leader_commit)
      assert is_nil(rpc.prev_log_term)

      assert %Types.AppendEntriesRpc{entries: entries} = rpc
      assert entries == []
    end

    test "AppendEntriesReply fields" do
      reply = %Types.AppendEntriesReply{
        term: 2,
        success: true,
        next_index: 12,
        last_index: 11,
        last_term: 2
      }

      assert reply.success
      assert reply.next_index == 12
      assert %Types.AppendEntriesReply{success: true} = reply
    end

    test "RequestVoteRpc and RequestVoteResult" do
      rpc = %Types.RequestVoteRpc{
        term: 3,
        candidate_id: {:s2, :node2@host},
        last_log_index: 15,
        last_log_term: 2
      }

      assert rpc.last_log_index == 15
      assert rpc.last_log_term == 2
      assert rpc.candidate_id == {:s2, :node2@host}

      result = %Types.RequestVoteResult{term: 3, vote_granted: true}
      assert result.vote_granted

      assert %Types.RequestVoteResult{vote_granted: false} = %Types.RequestVoteResult{
               vote_granted: false
             }
    end

    test "TimeoutNowRpc carries term and leader" do
      rpc = %Types.TimeoutNowRpc{term: 7, leader_id: {:s1, node()}}

      assert rpc.term == 7
      assert rpc.leader_id == {:s1, node()}
      assert %Types.TimeoutNowRpc{term: 7} = rpc
    end

    test "PreVoteRpc defaults version to 1" do
      rpc = %Types.PreVoteRpc{term: 4, candidate_id: {:s1, node()}, token: make_ref()}

      assert rpc.version == 1
      assert rpc.term == 4
      assert is_reference(rpc.token)
      assert is_nil(rpc.machine_version)
      assert is_nil(rpc.last_log_index)
      assert is_nil(rpc.last_log_term)
    end

    test "PreVoteResult fields" do
      token = make_ref()
      result = %Types.PreVoteResult{term: 7, token: token, vote_granted: false}

      assert result.token == token
      refute result.vote_granted
    end

    test "InstallSnapshotRpc and InstallSnapshotResult" do
      rpc = %Types.InstallSnapshotRpc{
        term: 1,
        leader_id: {:leader, node()},
        meta: %{index: 10},
        chunk_state: :first,
        data: "chunk"
      }

      assert rpc.meta == %{index: 10}
      assert rpc.data == "chunk"

      result = %Types.InstallSnapshotResult{term: 1, last_index: 10, last_term: 1}
      assert result.last_index == 10
      assert result.last_term == 1
    end

    test "HeartbeatRpc and HeartbeatReply" do
      rpc = %Types.HeartbeatRpc{query_index: 4, term: 2, leader_id: {:l, node()}}
      assert rpc.query_index == 4

      reply = %Types.HeartbeatReply{query_index: 4, term: 2}
      assert reply.query_index == 4
      assert Enum.sort(Map.keys(reply)) == [:__struct__, :query_index, :term]
    end

    test "InfoRpc and InfoReply default info" do
      rpc = %Types.InfoRpc{from: {:s, node()}, term: 1, keys: [:a, :b]}
      assert rpc.keys == [:a, :b]

      reply = %Types.InfoReply{from: {:s, node()}, term: 1, keys: []}
      assert reply.info == %{}
    end
  end

  describe "struct identity" do
    test "each message struct has a distinct __struct__" do
      structs = [
        %Types.AppendEntriesRpc{},
        %Types.AppendEntriesReply{},
        %Types.RequestVoteRpc{},
        %Types.RequestVoteResult{},
        %Types.PreVoteRpc{},
        %Types.PreVoteResult{},
        %Types.InstallSnapshotRpc{},
        %Types.InstallSnapshotResult{},
        %Types.HeartbeatRpc{},
        %Types.HeartbeatReply{},
        %Types.InfoRpc{},
        %Types.InfoReply{}
      ]

      assert length(Enum.uniq(structs)) == 12
      assert structs |> Enum.map(& &1.__struct__) |> Enum.uniq() |> length() == 12
    end

    test "structs are plain maps with expected keys" do
      assert Enum.sort(Map.keys(%Types.AppendEntriesRpc{})) ==
               Enum.sort([
                 :__struct__,
                 :term,
                 :leader_id,
                 :leader_commit,
                 :prev_log_index,
                 :prev_log_term,
                 :entries
               ])

      assert Enum.sort(Map.keys(%Types.InfoReply{})) == [:__struct__, :from, :info, :keys, :term]
    end
  end
end
