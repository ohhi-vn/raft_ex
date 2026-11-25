defmodule RaftEx.Domain.SeqTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Seq

  describe "append/2" do
    test "starts a new sequence" do
      assert Seq.append(1, []) == [1]
    end

    test "compresses two consecutive indices into a range" do
      assert Seq.append(5, [4, 3]) == [{3, 5}]
      assert Seq.append(2, [1]) == [2, 1]
    end

    test "extends an existing range" do
      assert Seq.append(6, [{3, 5}]) == [{3, 6}]
      assert Seq.append(6, [{3, 5}, 1]) == [{3, 6}, 1]
    end

    test "prepends an index above the current maximum" do
      assert Seq.append(9, [{3, 5}]) == [9, {3, 5}]
    end

    test "ignores an index already covered" do
      assert Seq.append(4, [{3, 5}]) == [{3, 5}]
    end
  end

  describe "from_list/1" do
    test "builds a sparse sequence from an unsorted list" do
      assert Seq.from_list([3, 1, 2]) == [{1, 3}]
      assert Seq.from_list([5, 2, 1, 3, 4]) == [{1, 5}]
    end

    test "handles gaps and duplicates" do
      assert Seq.from_list([5, 3, 3, 1]) == [5, 3, 1]
    end

    test "empty list" do
      assert Seq.from_list([]) == []
    end
  end

  describe "floor/2" do
    test "keeps integer indices at or above the floor" do
      assert Seq.floor(3, [5, 4, 3, 2, 1]) == [5, 4, 3]
    end

    test "truncates ranges at the floor" do
      assert Seq.floor(3, [{1, 5}]) == [{3, 5}]
      assert Seq.floor(4, [{1, 5}]) == [5, 4]
      assert Seq.floor(5, [{1, 5}]) == [5]
      assert Seq.floor(6, [{1, 5}]) == []
    end

    test "nil / empty" do
      assert Seq.floor(1, []) == []
    end
  end

  describe "limit/2" do
    test "drops integer indices above the ceiling" do
      assert Seq.limit(3, [5, 4, 3, 2, 1]) == [3, 2, 1]
    end

    test "truncates ranges at the ceiling" do
      assert Seq.limit(3, [{1, 5}]) == [{1, 3}]
      assert Seq.limit(1, [{1, 5}]) == [1]
      assert Seq.limit(2, [{1, 5}]) == [2, 1]
      assert Seq.limit(0, [{1, 5}]) == []
    end

    test "keeps a range below the ceiling unchanged" do
      assert Seq.limit(9, [{1, 5}]) == [{1, 5}]
    end
  end

  describe "add/2" do
    test "nil sequences" do
      assert Seq.add([], [1, 2]) == [1, 2]
      assert Seq.add([1, 2], []) == [1, 2]
      assert Seq.add([], []) == []
    end

    test "adds entries into a truncated base" do
      assert Seq.add([1], [2]) == [1]
      assert Seq.add([2, 1], [3]) == [2, 1]
    end
  end

  describe "fold/3, expand/1, length/1" do
    test "fold visits every index in ascending order" do
      assert Seq.fold(fn i, acc -> acc + i end, 0, [{1, 3}, 5]) == 11
    end

    test "expand returns indices high to low" do
      assert Seq.expand([{1, 3}]) == [3, 2, 1]
      assert Seq.expand([5, {1, 2}]) == [5, 2, 1]
    end

    test "length counts expanded indices" do
      assert Seq.length([]) == 0
      assert Seq.length([{1, 3}, 5]) == 4
    end
  end

  describe "first/1, last/1, range/1" do
    test "first returns the lowest index" do
      assert Seq.first([]) == nil
      assert Seq.first([{1, 3}]) == 1
      assert Seq.first([{1, 3}, 5]) == 5
    end

    test "last returns the highest index" do
      assert Seq.last([]) == nil
      assert Seq.last([{1, 3}]) == 3
      assert Seq.last([5, {1, 3}]) == 5
    end

    test "range spans the whole sequence" do
      assert Seq.range([]) == nil
      assert Seq.range([{1, 3}]) == {1, 3}
      assert Seq.range([5, {1, 3}]) == {1, 5}
    end
  end

  describe "in?/2" do
    test "membership including range contents" do
      assert Seq.in?(3, [5, 3, 1])
      assert Seq.in?(2, [{1, 3}])
      refute Seq.in?(9, [{1, 3}])
      refute Seq.in?(1, [])
    end
  end

  describe "in_range/2" do
    test "returns the indices inside the range" do
      assert Seq.in_range({2, 4}, [{1, 5}]) == [{2, 4}]
      assert Seq.in_range({1, 5}, [{1, 5}]) == [{1, 5}]
    end

    test "nil or empty inputs yield an empty sequence" do
      assert Seq.in_range(nil, [{1, 5}]) == []
      assert Seq.in_range({2, 4}, []) == []
    end
  end

  describe "has_overlap?/2" do
    test "detects overlap with integers and ranges" do
      assert Seq.has_overlap?({2, 4}, [5, 3, 1])
      assert Seq.has_overlap?({2, 4}, [{5, 9}, {1, 3}])
    end

    test "no overlap" do
      refute Seq.has_overlap?({6, 8}, [5, 3])
      refute Seq.has_overlap?({6, 8}, [{1, 3}])
      refute Seq.has_overlap?({2, 4}, [])
      refute Seq.has_overlap?(nil, [1, 2])
    end

    test "recurses past out-of-range entries to the empty tail" do
      refute Seq.has_overlap?({2, 4}, [9, 8])
      refute Seq.has_overlap?({2, 4}, [{9, 11}, {8, 10}])
    end
  end

  describe "iterator/1, next_iter/1, remove_prefix/2" do
    test "iterator reverses into ascending order" do
      assert Seq.iterator([3, 2, 1]) == [1, 2, 3]
      assert Seq.iterator([]) == []
    end

    test "next_iter returns the lowest element and shrinks ranges" do
      assert Seq.next_iter([]) == :end_of_seq
      assert Seq.next_iter([3, 2, 1]) == {3, [2, 1]}
      assert Seq.next_iter([{1, 5}]) == {1, [{2, 5}]}
      assert Seq.next_iter([{1, 1}]) == {1, []}
    end

    test "remove_prefix drops the leading elements" do
      assert Seq.remove_prefix([2, 1], [3, 2, 1]) == {:ok, [3]}
      assert Seq.remove_prefix([2, 1], [5, 4, 3]) == {:ok, [{3, 5}]}
      assert Seq.remove_prefix([3, 4], [3, 2, 1]) == {:error, :not_prefix}
    end

    test "remove_prefix returns an empty sequence when the prefix consumes the whole seq" do
      assert Seq.remove_prefix([3, 2, 1], [2, 1]) == {:ok, []}
    end
  end
end
