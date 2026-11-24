defmodule RaftEx.Domain.LruCacheTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.LruCache

  defp put(cache, key, value), do: LruCache.insert(key, value, cache)

  describe "new/2" do
    test "creates an empty cache with the given size and handler" do
      handler = fn _ -> :ok end
      cache = LruCache.new(3, handler)

      assert cache.max_size == 3
      assert cache.items == []
      assert cache.handler == handler
      assert LruCache.max_size(cache) == 3
      assert LruCache.size(cache) == 0
    end

    test "accepts a nil handler" do
      assert LruCache.new(2, nil).handler == nil
    end
  end

  describe "insert/3 and fetch/2" do
    test "fetch misses on an empty cache" do
      assert LruCache.fetch(:a, LruCache.new(2, nil)) == :error
    end

    test "insert and fetch values" do
      cache = LruCache.new(2, nil) |> put(:a, 1) |> put(:b, 2)

      assert LruCache.size(cache) == 2
      assert {:ok, 2, _cache} = LruCache.fetch(:b, cache)
      assert {:ok, 1, _cache} = LruCache.fetch(:a, cache)
    end

    test "fetch promotes an older key to the front" do
      cache = LruCache.new(3, nil) |> put(:a, 1) |> put(:b, 2)
      assert {:ok, 1, cache} = LruCache.fetch(:a, cache)
      assert Enum.map(cache.items, &elem(&1, 0)) == [:a, :b]
    end

    test "fetch on the head returns the same state" do
      cache = LruCache.new(3, nil) |> put(:a, 1)
      assert {:ok, 1, ^cache} = LruCache.fetch(:a, cache)
    end

    test "insert evicts the least recently used entry and calls the handler" do
      parent = self()
      handler = fn evicted -> send(parent, {:evicted, evicted}) end
      cache = LruCache.new(2, handler) |> put(:a, 1) |> put(:b, 2)

      cache = LruCache.insert(:c, 3, cache)

      assert LruCache.size(cache) == 2
      refute match?({:ok, _, _}, LruCache.fetch(:a, cache))
      assert {:ok, 3, _} = LruCache.fetch(:c, cache)
      assert_received {:evicted, {:a, 1}}
    end

    test "fetch before insert prevents eviction of the promoted key" do
      cache = LruCache.new(2, nil) |> put(:a, 1) |> put(:b, 2)
      {:ok, 1, cache} = LruCache.fetch(:a, cache)
      cache = LruCache.insert(:c, 3, cache)

      assert LruCache.fetch(:b, cache) == :error
      assert {:ok, 1, _} = LruCache.fetch(:a, cache)
      assert {:ok, 3, _} = LruCache.fetch(:c, cache)
    end
  end

  describe "evict/2 and evict_all/1" do
    test "evict removes a key and calls the handler" do
      parent = self()
      handler = fn evicted -> send(parent, {:evicted, evicted}) end
      cache = LruCache.new(3, handler) |> put(:a, 1)

      assert {{:a, 1}, cache} = LruCache.evict(:a, cache)
      assert LruCache.size(cache) == 0
      assert_received {:evicted, {:a, 1}}
    end

    test "evict returns error for a missing key" do
      assert LruCache.evict(:missing, LruCache.new(3, nil)) == :error
    end

    test "evict_all clears the cache and calls the handler for each entry" do
      parent = self()
      handler = fn evicted -> send(parent, {:evicted, evicted}) end
      cache = LruCache.new(3, handler) |> put(:a, 1) |> put(:b, 2)

      cache = LruCache.evict_all(cache)

      assert LruCache.size(cache) == 0
      assert_received {:evicted, {:a, 1}}
      assert_received {:evicted, {:b, 2}}
    end

    test "evict_all with nil handler still clears" do
      cache = LruCache.new(3, nil) |> put(:a, 1)
      assert LruCache.size(LruCache.evict_all(cache)) == 0
    end
  end
end
