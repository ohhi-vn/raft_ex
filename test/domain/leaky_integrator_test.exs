defmodule RaftEx.Domain.LeakyIntegratorTest do
  use ExUnit.Case, async: true

  alias RaftEx.Domain.LeakyIntegrator

  defp add(li, amount, ts), do: LeakyIntegrator.update(amount, ts, li)
  defp add_now(li, amount), do: LeakyIntegrator.update(amount, li)

  describe "new/1" do
    test "creates a zeroed integrator" do
      li = LeakyIntegrator.new(1_000)

      assert li.decay_time_ms == 1_000
      assert li.value == 0.0
      assert li.last_update == nil
    end

    test "requires a positive decay time" do
      assert_raise FunctionClauseError, fn -> LeakyIntegrator.new(0) end
      assert_raise FunctionClauseError, fn -> LeakyIntegrator.new(-1) end
    end
  end

  describe "update/3" do
    test "first update sets the value and timestamp" do
      li = LeakyIntegrator.new(1_000) |> add(10, 100)

      assert li.value == 10.0
      assert li.last_update == 100
    end

    test "subsequent updates decay before adding" do
      li = LeakyIntegrator.new(1_000) |> add(10, 100)
      li = add(li, 5, 200)

      expected = 10.0 * :math.exp(-100 / 1_000) + 5.0
      assert li.value == expected
      assert li.last_update == 200
    end

    test "a backwards clock adds without decaying" do
      li = LeakyIntegrator.new(1_000) |> add(10, 100)
      li = add(li, 5, 50)

      assert li.value == 15.0
      assert li.last_update == 100
    end

    test "accepts float amounts" do
      li = LeakyIntegrator.new(100) |> add(2.5, 0)
      assert li.value == 2.5
    end
  end

  describe "update/2 with the monotonic clock" do
    test "uses the current clock" do
      li = LeakyIntegrator.new(1_000) |> add_now(3)

      assert li.value == 3.0
      assert is_integer(li.last_update)
    end
  end

  describe "read/1 and read/2" do
    test "reads zero before the first update" do
      assert LeakyIntegrator.read(0, LeakyIntegrator.new(1_000)) == 0.0
    end

    test "reads the decayed value at a timestamp" do
      li = LeakyIntegrator.new(1_000) |> add(10, 100)

      assert LeakyIntegrator.read(100, li) == 10.0
      assert LeakyIntegrator.read(200, li) == 10.0 * :math.exp(-100 / 1_000)
      assert LeakyIntegrator.read(50, li) == 10.0
    end

    test "read/1 uses the current clock" do
      li = LeakyIntegrator.new(1_000) |> add_now(7)
      assert is_number(LeakyIntegrator.read(li))
    end
  end

  describe "rate/1 and rate/2" do
    test "scales the decayed value to units per second" do
      li = LeakyIntegrator.new(1_000) |> add(10, 100)

      assert LeakyIntegrator.rate(100, li) == 10.0
      assert LeakyIntegrator.rate(200, li) == 10.0 * :math.exp(-100 / 1_000)
    end

    test "rate/1 uses the current clock" do
      li = LeakyIntegrator.new(500) |> add_now(2)
      assert is_number(LeakyIntegrator.rate(li))
    end
  end

  describe "reset/1" do
    test "returns a zeroed integrator that keeps its decay time" do
      li = LeakyIntegrator.new(250) |> add(9, 10)
      reset = LeakyIntegrator.reset(li)

      assert reset.decay_time_ms == 250
      assert reset.value == 0.0
      assert reset.last_update == nil
      assert LeakyIntegrator.read(100, reset) == 0.0
    end
  end
end
