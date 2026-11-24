defmodule RaftEx.Domain.RangeTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Range

  describe "new/1 and new/2" do
    test "new/1 creates a single-point range" do
      assert Range.new(5) == {5, 5}
      assert Range.new(0) == {0, 0}
    end

    test "new/2 creates a range and rejects inverted bounds" do
      assert Range.new(2, 6) == {2, 6}
      assert Range.new(3, 3) == {3, 3}
      assert Range.new(6, 2) == nil
      assert Range.new(-1, 2) == {-1, 2}
      assert Range.new(:a, :b) == nil
      assert Range.new(1, :b) == nil
    end
  end

  describe "add/2" do
    test "nil is an identity element" do
      assert Range.add(nil, {1, 2}) == {1, 2}
      assert Range.add({1, 2}, nil) == {1, 2}
      assert Range.add(nil, nil) == nil
    end

    test "merges overlapping and adjacent ranges" do
      assert Range.add({2, 4}, {5, 8}) == {2, 8}
      assert Range.add({5, 8}, {2, 4}) == {2, 8}
      assert Range.add({2, 10}, {4, 6}) == {2, 10}
    end

    test "keeps the first range when the second has a gap" do
      assert Range.add({2, 4}, {6, 8}) == {2, 4}
    end

    test "leaves disjoint ranges unchanged" do
      assert Range.add({2, 4}, {7, 9}) == {2, 4}
      assert Range.add({7, 9}, {2, 4}) == {7, 9}
    end
  end

  describe "combine/2" do
    test "nil is an identity element" do
      assert Range.combine(nil, {1, 2}) == {1, 2}
    end

    test "combines ranges while keeping the first range's end" do
      assert Range.combine({1, 2}, {3, 9}) == {1, 2}
      assert Range.combine({5, 8}, {2, 4}) == {2, 8}
    end

    test "passes through a non-range second argument" do
      assert Range.combine({1, 2}, nil) == {1, 2}
    end
  end

  describe "in?/2" do
    test "detects membership including boundaries" do
      assert Range.in?(2, {1, 3})
      assert Range.in?(1, {1, 3})
      assert Range.in?(3, {1, 3})
      refute Range.in?(0, {1, 3})
      refute Range.in?(4, {1, 3})
    end

    test "nil never contains an index" do
      refute Range.in?(1, nil)
    end
  end

  describe "limit/2" do
    test "returns nil when ceiling is below or at start" do
      assert Range.limit(1, {1, 3}) == nil
      assert Range.limit(0, {1, 3}) == nil
    end

    test "truncates the end when ceiling is within the range" do
      assert Range.limit(3, {1, 5}) == {1, 2}
      assert Range.limit(5, {1, 5}) == {1, 4}
    end

    test "leaves the range unchanged when ceiling is above the end" do
      assert Range.limit(9, {1, 5}) == {1, 5}
    end
  end

  describe "truncate/2" do
    test "returns nil when the range is entirely at or below the cutoff" do
      assert Range.truncate(6, {4, 6}) == nil
      assert Range.truncate(9, {4, 6}) == nil
    end

    test "keeps a range above the cutoff unchanged" do
      assert Range.truncate(3, {4, 6}) == {4, 6}
    end

    test "drops the lower part of a partially covered range" do
      assert Range.truncate(4, {2, 6}) == {5, 6}
      assert Range.truncate(2, {2, 6}) == {3, 6}
    end

    test "returns nil when the whole range is below the cutoff" do
      assert Range.truncate(9, {2, 6}) == nil
    end
  end

  describe "size/1" do
    test "counts inclusive elements" do
      assert Range.size(nil) == 0
      assert Range.size({5, 5}) == 1
      assert Range.size({2, 6}) == 5
    end
  end

  describe "extend/2" do
    test "extends an adjacent range" do
      assert Range.extend(6, {2, 5}) == {2, 6}
    end

    test "creates a range from nil" do
      assert Range.extend(3, nil) == {3, 3}
    end

    test "raises for a non-adjacent index" do
      assert_raise RuntimeError, ~r/cannot_extend/, fn -> Range.extend(9, {2, 5}) end
    end
  end

  describe "overlap/2" do
    test "returns the intersection" do
      assert Range.overlap({2, 6}, {4, 9}) == {4, 6}
      assert Range.overlap({4, 9}, {2, 6}) == {4, 6}
      assert Range.overlap({1, 3}, {5, 7}) == nil
    end

    test "returns nil for non-range arguments" do
      assert Range.overlap({1, 3}, nil) == nil
      assert Range.overlap(nil, {1, 3}) == nil
    end
  end

  describe "subtract/2" do
    test "nil handling" do
      assert Range.subtract({1, 3}, nil) == []
      assert Range.subtract(nil, {1, 3}) == [{1, 3}]
    end

    test "removes an overlapping sub-range" do
      assert Range.subtract({3, 5}, {1, 10}) == [{1, 2}, {6, 10}]
      assert Range.subtract({1, 3}, {1, 10}) == [{4, 10}]
      assert Range.subtract({8, 10}, {1, 10}) == [{1, 7}]
      assert Range.subtract({2, 8}, {1, 10}) == [{1, 1}, {9, 10}]
    end

    test "leaves a range unchanged when there is no overlap" do
      assert Range.subtract({20, 25}, {1, 10}) == [{1, 10}]
    end
  end

  describe "fold/3" do
    test "folds over the inclusive range" do
      assert Range.fold({1, 4}, fn i, acc -> acc + i end, 0) == 10
    end

    test "nil is an identity element" do
      assert Range.fold(nil, fn i, acc -> acc + i end, 42) == 42
    end
  end
end
