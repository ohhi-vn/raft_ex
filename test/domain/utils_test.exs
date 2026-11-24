defmodule RaftEx.Domain.UtilsTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.Utils

  describe "coercions" do
    test "to_list/1" do
      assert Utils.to_list(:abc) == ~c"abc"
      assert Utils.to_list("abc") == ~c"abc"
      assert Utils.to_list(123) == ~c"123"
      assert Utils.to_list([?a, ?b]) == [?a, ?b]
    end

    test "to_binary/1" do
      assert Utils.to_binary("abc") == "abc"
      assert Utils.to_binary(:abc) == "abc"
      assert Utils.to_binary(123) == "123"
      assert Utils.to_binary(~c"abc") == "abc"
    end

    test "to_atom/1" do
      assert Utils.to_atom(:abc) == :abc
      assert Utils.to_atom("some_test_atom") == :some_test_atom
      assert Utils.to_atom(~c"another_test_atom") == :another_test_atom
    end
  end

  describe "server id helpers" do
    test "ra_server_id_to_local_name/1" do
      assert Utils.ra_server_id_to_local_name({:server1, :node@host}) == :server1
      assert Utils.ra_server_id_to_local_name(:server2) == :server2
    end

    test "ra_server_id_node/1" do
      assert Utils.ra_server_id_node({:server1, :node@host}) == :node@host
      assert Utils.ra_server_id_node(:server2) == node()
    end
  end

  describe "make_uid/0 and make_uid/1" do
    test "generates a 12-character alphanumeric suffix" do
      uid = Utils.make_uid()
      assert byte_size(uid) == 12
      assert Regex.match?(~r/^[A-Z0-9]{12}$/, uid)
    end

    test "accepts binary, atom and charlist prefixes" do
      for prefix <- ["pre-", :pre_, ~c"pre_"] do
        uid = Utils.make_uid(prefix)
        assert byte_size(uid) == byte_size(Utils.to_binary(prefix)) + 12
        assert String.starts_with?(uid, Utils.to_binary(prefix))
      end
    end

    test "generates distinct UIDs" do
      refute Utils.make_uid() == Utils.make_uid()
    end
  end

  describe "base64 URI validation" do
    test "validate_base64uri/1" do
      assert Utils.validate_base64uri("ABCxyz019_-=")
      refute Utils.validate_base64uri("")
      refute Utils.validate_base64uri("has space")
      refute Utils.validate_base64uri("has+plus")
      refute Utils.validate_base64uri("has/slash")
    end

    test "derive_safe_string/2 filters and truncates" do
      assert Utils.derive_safe_string("a b+c/d_e", 20) == "abcd_e"
      assert Utils.derive_safe_string("abcdef", 3) == "abc"
      assert Utils.derive_safe_string("!@#", 3) == ""
    end
  end

  describe "zero-padded formatting" do
    test "zpad_hex/1" do
      assert Utils.zpad_hex(0) == String.duplicate("0", 16)
      assert Utils.zpad_hex(0xABC) == "0000000000000ABC"
      assert Utils.zpad_hex(0xFFFFFFFFFFFFFFFF) == "FFFFFFFFFFFFFFFF"
    end

    test "zpad_filename/3" do
      assert Utils.zpad_filename("", "dat", 5) == "0000000000000005.dat"
      assert Utils.zpad_filename("seg", "dat", 42) == "seg_0000000000000042.dat"
    end

    test "zpad_filename_incr/1" do
      path = "/tmp/seg_0000000000000042.dat"
      assert Utils.zpad_filename_incr(path) == "/tmp/seg_0000000000000043.dat"
      assert Utils.zpad_filename_incr("/tmp/no_number.dat") == nil
      assert Utils.zpad_filename_incr("0000000000000009") == "./0000000000000010"
    end

    test "zpad_extract_num/1" do
      assert Utils.zpad_extract_num("seg_0000000000000042.dat") == 42
    end
  end

  describe "misc helpers" do
    test "cons/2" do
      assert Utils.cons(1, [2, 3]) == [1, 2, 3]
    end

    test "lists_shuffle/1 preserves all elements" do
      list = Enum.to_list(1..10)
      assert Utils.lists_shuffle(list) |> Enum.sort() == list
      assert Utils.lists_shuffle([]) == []
    end
  end

  describe "partition_parallel/3" do
    test "partitions elements by the boolean result" do
      assert {:ok, successes, failures} =
               Utils.partition_parallel(fn e -> rem(e, 2) == 0 end, [1, 2, 3, 4])

      assert Enum.sort(successes) == [2, 4]
      assert Enum.sort(failures) == [1, 3]
    end

    test "handles an empty input" do
      assert Utils.partition_parallel(& &1, []) == {:ok, [], []}
    end

    test "records process exits as failures" do
      assert {:ok, successes, failures} =
               Utils.partition_parallel(
                 fn e ->
                   if e == :boom, do: exit(:crashed), else: true
                 end,
                 [:ok, :boom]
               )

      assert successes == [:ok]
      assert failures == [{:boom, :crashed}]
    end

    test "times out slow functions" do
      assert {:error, {:partition_parallel_timeout, [], []}} =
               Utils.partition_parallel(
                 fn _ -> Process.sleep(200) end,
                 [:slow],
                 5
               )
    end
  end
end
