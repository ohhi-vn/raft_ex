defmodule RaftEx.Domain.DecisionTableTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.{LruCache, Range, Seq}
  alias RaftEx.Domain.Server.{Cluster, Election}

  # Pure decision functions deserve a test per boundary value.  Tables keep the
  # intent obvious while giving every case its own name in the test report.

  describe "Election.votes_needed/1" do
    for total <- 0..20 do
      test "majority of #{total} voters" do
        assert Election.votes_needed(unquote(total)) == div(unquote(total), 2) + 1
      end
    end
  end

  describe "Election.can_still_win?/2" do
    @can_win_cases [
      {0, 1, true},
      {1, 1, false},
      {0, 3, true},
      {1, 3, true},
      {2, 3, false},
      {3, 3, false},
      {0, 4, true},
      {1, 4, true},
      {2, 4, true},
      {3, 4, false},
      {4, 4, false}
    ]

    for {votes, total, expected} <- @can_win_cases do
      test "#{votes} votes out of #{total} -> #{expected}" do
        assert Election.can_still_win?(unquote(votes), unquote(total)) == unquote(expected)
      end
    end
  end

  describe "Election.can_still_win?/3" do
    @can_win_with_rejections [
      {1, 3, 0, true},
      {1, 3, 1, true},
      {1, 3, 2, false},
      {0, 3, 1, true},
      {0, 3, 2, false},
      {0, 3, 3, false},
      {2, 5, 0, true},
      {2, 5, 1, true},
      {2, 5, 2, true},
      {1, 5, 2, true},
      {1, 5, 3, false},
      {3, 7, 3, true},
      {2, 1, 0, false}
    ]

    for {votes, total, rejected, expected} <- @can_win_with_rejections do
      test "#{votes} votes, #{rejected} rejected, #{total} voters -> #{expected}" do
        assert Election.can_still_win?(unquote(votes), unquote(total), unquote(rejected)) ==
                 unquote(expected)
      end
    end
  end

  describe "Election.evaluate_election_result/1" do
    @election_results [
      {1, 1, 0, :won},
      {2, 3, 0, :won},
      {3, 5, 0, :won},
      {2, 5, 0, :ongoing},
      {2, 5, 1, :ongoing},
      {2, 5, 2, :ongoing},
      {1, 5, 3, :lost},
      {0, 3, 3, :lost},
      {0, 1, 1, :lost},
      {4, 7, 2, :won},
      {2, 7, 4, :lost},
      {0, 7, 0, :ongoing}
    ]

    for {votes, total, rejected, expected} <- @election_results do
      test "#{votes}/#{total} with #{rejected} rejected -> #{expected}" do
        context = %{
          state: :candidate,
          term: 1,
          votes_received: MapSet.new(Enum.take(Stream.iterate(1, &(&1 + 1)), unquote(votes))),
          votes_rejected: MapSet.new(Enum.take(Stream.iterate(1, &(&1 + 1)), unquote(rejected))),
          total_voters: unquote(total),
          last_log_index: 0,
          last_log_term: 0
        }

        assert Election.evaluate_election_result(context) == unquote(expected)
      end
    end
  end

  describe "Cluster.agreed_commit/1" do
    @agreed_commit_cases [
      {[5], 5},
      {[5, 3], 3},
      {[5, 4, 3], 4},
      {[5, 4, 3, 2], 3},
      {[5, 4, 3, 2, 1], 3},
      {[10, 7, 9], 9},
      {[1, 1, 1], 1},
      {[0, 0, 0, 0, 0], 0},
      {[9, 8, 7, 6, 5, 4, 3], 6},
      {[3, 2], 2}
    ]

    for {indexes, expected} <- @agreed_commit_cases do
      test "#{inspect(indexes)} -> #{expected}" do
        assert Cluster.agreed_commit(unquote(indexes)) == unquote(expected)
      end
    end
  end

  describe "Cluster membership helpers" do
    test "new/2 seeds self and every peer with a default voter state" do
      self_id = {:s1, node()}
      cluster = Cluster.new(self_id, [self_id, {:s2, node()}, {:s3, node()}])

      assert Map.keys(cluster) |> Enum.sort() ==
               Enum.sort([self_id, {:s2, node()}, {:s3, node()}])

      assert Cluster.voter_count(cluster) == 3
      assert Cluster.new_peer().match_index == 0
    end

    test "new/2 merges partial peer states over defaults" do
      peer = {:s2, node()}
      cluster = Cluster.new({:s1, node()}, %{peer => %{match_index: 7}})

      assert Cluster.get_peer(peer, cluster).match_index == 7
      assert Cluster.get_peer(peer, cluster).next_index == 1
    end

    test "update_peer/3 merges rather than replacing peer state" do
      peer = {:s2, node()}
      base = Cluster.new({:s1, node()}, [peer])
      cluster = Cluster.update_peer(peer, %{match_index: 4}, base)

      assert Cluster.get_peer(peer, cluster).match_index == 4
      assert Cluster.get_peer(peer, cluster).voter_status == %{membership: :voter, uid: nil}
    end

    for membership <- [:voter, :promotable, :non_voter] do
      test "get_membership/4 returns #{membership} when uid matches" do
        peer = {:s2, node()}
        uid = String.to_atom("uid_#{unquote(membership)}")

        cluster =
          Cluster.update_peer(
            peer,
            %{voter_status: %{membership: unquote(membership), uid: uid}},
            Cluster.new({:s1, node()}, [peer])
          )

        assert Cluster.get_membership(cluster, peer, uid, :voter) == unquote(membership)
      end

      test "get_membership/4 reports #{membership} regardless of uid" do
        peer = {:s2, node()}

        cluster =
          Cluster.update_peer(
            peer,
            %{voter_status: %{membership: unquote(membership), uid: :other}},
            Cluster.new({:s1, node()}, [peer])
          )

        assert Cluster.get_membership(cluster, peer, :missing, :voter) == unquote(membership)
        assert Cluster.get_membership(cluster, peer, :other, :voter) == unquote(membership)
      end
    end

    test "voter_count/1 and voters/1 ignore non-voters" do
      self_id = {:s1, node()}
      p2 = {:s2, node()}
      p3 = {:s3, node()}

      cluster =
        Cluster.new(self_id, [p2, p3])
        |> then(
          &Cluster.update_peer(p2, %{voter_status: %{membership: :non_voter, uid: nil}}, &1)
        )

      assert Cluster.voters(cluster) == [self_id, p3]
      assert Cluster.voter_count(cluster) == 2
    end

    test "voter_match_indexes/3 includes the leader and only voter peers" do
      self_id = {:s1, node()}
      p2 = {:s2, node()}
      p3 = {:s3, node()}

      cluster =
        Cluster.new(self_id, [p2, p3])
        |> then(&Cluster.update_peer(p2, %{match_index: 5}, &1))
        |> then(
          &Cluster.update_peer(
            p3,
            %{
              match_index: 9,
              voter_status: %{membership: :non_voter, uid: nil}
            },
            &1
          )
        )

      assert Enum.sort(Cluster.voter_match_indexes(self_id, cluster, 7)) == Enum.sort([7, 5])
    end

    test "reset_peer_statuses/1 clears every peer status" do
      self_id = {:s1, node()}

      cluster =
        Cluster.new(self_id, [{:s2, node()}])
        |> then(&Cluster.update_peer(self_id, %{status: :pipeline}, &1))
        |> Cluster.reset_peer_statuses()

      assert Enum.all?(cluster, fn {_id, peer} -> peer.status == :normal end)
    end

    test "reset_query_indexes/1 zeroes every query index" do
      self_id = {:s1, node()}

      cluster =
        Cluster.new(self_id, [{:s2, node()}])
        |> then(&Cluster.update_peer(self_id, %{query_index: 12}, &1))
        |> Cluster.reset_query_indexes()

      assert Enum.all?(cluster, fn {_id, peer} -> peer.query_index == 0 end)
    end
  end

  describe "Range helpers" do
    test "new/1 creates a single element range" do
      assert Range.new(3) == {3, 3}
    end

    test "new/2 creates a valid range" do
      assert Range.new(3, 5) == {3, 5}
    end

    test "new/2 rejects an inverted range" do
      assert Range.new(5, 3) == nil
    end

    for {left, right, merged} <- [
          {nil, {2, 4}, {2, 4}},
          {{2, 4}, nil, {2, 4}},
          {{2, 4}, {5, 7}, {2, 7}},
          {{2, 4}, {6, 7}, {2, 4}}
        ] do
      test "add(#{inspect(left)}, #{inspect(right)}) -> #{inspect(merged)}" do
        assert Range.add(unquote(left), unquote(right)) == unquote(merged)
      end
    end

    for {idx, range, expected} <- [
          {2, nil, false},
          {2, {2, 4}, true},
          {4, {2, 4}, true},
          {1, {2, 4}, false},
          {5, {2, 4}, false}
        ] do
      test "in?(#{idx}, #{inspect(range)}) -> #{expected}" do
        assert Range.in?(unquote(idx), unquote(range)) == unquote(expected)
      end
    end

    for {ceil, range, expected} <- [
          {4, {2, 6}, {2, 3}},
          {2, {2, 6}, nil},
          {9, {2, 6}, {2, 6}},
          {7, {2, 6}, {2, 6}}
        ] do
      test "limit(#{ceil}, #{inspect(range)}) -> #{inspect(expected)}" do
        assert Range.limit(unquote(ceil), unquote(range)) == unquote(expected)
      end
    end

    for {up_to, range, expected} <- [
          {4, {2, 6}, {5, 6}},
          {2, {2, 6}, {3, 6}},
          {7, {2, 6}, nil},
          {0, {2, 6}, {2, 6}}
        ] do
      test "truncate(#{up_to}, #{inspect(range)}) -> #{inspect(expected)}" do
        assert Range.truncate(unquote(up_to), unquote(range)) == unquote(expected)
      end
    end

    for {range, expected_size} <- [{nil, 0}, {{2, 2}, 1}, {{2, 6}, 5}] do
      test "size(#{inspect(range)}) -> #{expected_size}" do
        assert Range.size(unquote(range)) == unquote(expected_size)
      end
    end

    test "extend/2 grows a range by one" do
      assert Range.extend(5, {2, 4}) == {2, 5}
      assert Range.extend(3, nil) == {3, 3}
    end

    for {subtracted, from, expected} <- [
          {nil, {2, 6}, [{2, 6}]},
          {{2, 6}, nil, []},
          {{3, 4}, {2, 6}, [{2, 2}, {5, 6}]},
          {{2, 4}, {2, 6}, [{5, 6}]},
          {{5, 7}, {2, 6}, [{2, 4}]}
        ] do
      test "subtract(#{inspect(subtracted)}, #{inspect(from)}) -> #{inspect(expected)}" do
        assert Range.subtract(unquote(subtracted), unquote(from)) == unquote(expected)
      end
    end

    test "fold/3 applies the function over every index" do
      assert Range.fold({2, 4}, fn i, acc -> acc + i end, 0) == 9
      assert Range.fold(nil, fn i, acc -> acc + i end, 0) == 0
    end
  end

  describe "Seq helpers" do
    for values <- [[], [1], [1, 2], [1, 2, 3], [1, 3], [2, 4, 6], [0, 1, 2, 9]] do
      test "from_list/1 preserves #{inspect(values)} exactly" do
        seq = Seq.from_list(unquote(values))

        assert Seq.expand(seq) |> Enum.sort() == unquote(values)
        assert Seq.length(seq) == length(unquote(values))
      end
    end

    for {values, member, expected} <- [
          {[1, 2, 3], 2, true},
          {[1, 2, 3], 4, false},
          {[1, 3], 2, false},
          {[], 1, false},
          {[5, 6], 5, true}
        ] do
      test "in?(#{member}, #{inspect(values)}) -> #{expected}" do
        assert Seq.in?(unquote(member), Seq.from_list(unquote(values))) == unquote(expected)
      end
    end

    for {values, floor_idx, expected} <- [
          {[1, 2, 3], 2, [2, 3]},
          {[1, 2, 3], 1, [1, 2, 3]},
          {[1, 2, 3], 4, []},
          {[1, 4], 3, [4]}
        ] do
      test "floor(#{floor_idx}, #{inspect(values)}) -> #{inspect(expected)}" do
        assert Seq.floor(unquote(floor_idx), Seq.from_list(unquote(values)))
               |> Seq.expand()
               |> Enum.sort() == unquote(expected)
      end
    end

    for {values, ceil_idx, expected} <- [
          {[1, 2, 3], 2, [1, 2]},
          {[1, 2, 3], 3, [1, 2, 3]},
          {[1, 2, 3], 0, []},
          {[1, 4], 2, [1]}
        ] do
      test "limit(#{ceil_idx}, #{inspect(values)}) -> #{inspect(expected)}" do
        assert Seq.limit(unquote(ceil_idx), Seq.from_list(unquote(values)))
               |> Seq.expand()
               |> Enum.sort() == unquote(expected)
      end
    end

    for {values, range, expected} <- [
          {[1, 2, 3, 4], {2, 3}, [2, 3]},
          {[1, 2, 3, 4], {5, 6}, []},
          {[1, 2, 3, 4], {0, 1}, [1]},
          {[], {1, 2}, []}
        ] do
      test "in_range(#{inspect(range)}, #{inspect(values)}) -> #{inspect(expected)}" do
        assert Seq.in_range(unquote(range), Seq.from_list(unquote(values)))
               |> Seq.expand()
               |> Enum.sort() == unquote(expected)
      end
    end

    for {values, range, expected} <- [
          {[1, 2, 3], {2, 3}, true},
          {[1, 2, 3], {4, 5}, false},
          {[1, 3], {2, 2}, false},
          {[], {1, 2}, false}
        ] do
      test "has_overlap?(#{inspect(range)}, #{inspect(values)}) -> #{expected}" do
        assert Seq.has_overlap?(unquote(range), Seq.from_list(unquote(values))) ==
                 unquote(expected)
      end
    end

    test "first/1 and last/1 report the ends of a compacted sequence" do
      seq = Seq.from_list([2, 3, 4])
      assert Seq.first(seq) == 2
      assert Seq.last(seq) == 4
      assert Seq.first([]) == nil
      assert Seq.last([]) == nil
    end

    test "add/2 returns the added prefix when it precedes the base" do
      assert Seq.add([1], [2]) == [1]
      assert Seq.add([2, 1], [3]) == [2, 1]
      assert Seq.add([], [1, 2]) == [1, 2]
      assert Seq.add([1, 2], []) == [1, 2]
    end

    test "remove_prefix/2 returns the remaining suffix" do
      assert {:ok, rest} = Seq.remove_prefix(Seq.from_list([1, 2]), Seq.from_list([1, 2, 3]))
      assert Seq.expand(rest) == [3]
    end

    test "remove_prefix/2 rejects a non-prefix" do
      assert {:error, :not_prefix} =
               Seq.remove_prefix(Seq.from_list([2]), Seq.from_list([1, 2, 3]))
    end
  end

  describe "LruCache" do
    test "new/2 starts empty" do
      cache = LruCache.new(3, nil)
      assert LruCache.size(cache) == 0
      assert LruCache.max_size(cache) == 3
    end

    test "fetch/2 reports a miss without changing the cache" do
      cache = LruCache.insert(:a, 1, LruCache.new(3, nil))
      assert LruCache.fetch(:missing, cache) == :error
      assert LruCache.size(cache) == 1
    end

    test "fetch/2 promotes the most recently used entry" do
      cache =
        LruCache.new(3, nil)
        |> then(&LruCache.insert(:a, 1, &1))
        |> then(&LruCache.insert(:b, 2, &1))
        |> then(&LruCache.insert(:c, 3, &1))

      assert {:ok, 1, cache} = LruCache.fetch(:a, cache)
      assert [{:a, 1} | _] = cache.items
    end

    test "insert/3 evicts the least recently used entry" do
      cache = LruCache.new(2, fn entry -> send(self(), {:evicted, entry}) end)

      cache =
        cache
        |> then(&LruCache.insert(:a, 1, &1))
        |> then(&LruCache.insert(:b, 2, &1))
        |> then(&LruCache.insert(:c, 3, &1))

      assert LruCache.size(cache) == 2
      assert_received {:evicted, {:a, 1}}
      assert LruCache.fetch(:a, cache) == :error
      assert {:ok, 3, _} = LruCache.fetch(:c, cache)
    end

    test "evict/2 removes one entry and reports it" do
      cache =
        LruCache.new(3, nil)
        |> then(&LruCache.insert(:a, 1, &1))
        |> then(&LruCache.insert(:b, 2, &1))

      assert {{:a, 1}, cache} = LruCache.evict(:a, cache)
      assert LruCache.size(cache) == 1
      assert LruCache.evict(:a, cache) == :error
    end

    test "evict_all/1 clears the cache and calls the handler" do
      cache =
        LruCache.new(3, fn entry -> send(self(), {:evicted_all, entry}) end)
        |> then(&LruCache.insert(:a, 1, &1))
        |> then(&LruCache.insert(:b, 2, &1))

      cache = LruCache.evict_all(cache)

      assert LruCache.size(cache) == 0
      assert_received {:evicted_all, {:a, 1}}
      assert_received {:evicted_all, {:b, 2}}
    end
  end
end
