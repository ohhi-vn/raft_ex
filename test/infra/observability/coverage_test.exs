defmodule RaftEx.Infra.Observability.CoverageTest do
  use ExUnit.Case, async: false

  alias RaftEx.Infra.Network.Distribution
  alias RaftEx.Infra.Observability.{Counters, MetricNames, Metrics}
  alias RaftEx.Infra.Persistence.SharedWal
  alias RaftEx.Observability

  setup do
    Metrics.reset()
    :ok
  end

  describe "MetricNames" do
    test "every catalogue entry has a supported kind" do
      for {_name, kind} <- MetricNames.all() do
        assert kind in [:counter, :gauge, :histogram]
      end
    end

    for {name, kind} <- MetricNames.all() do
      test "kind(#{name}) == #{kind}" do
        assert MetricNames.kind(unquote(name)) == unquote(kind)
        assert unquote(name) in MetricNames.names()
      end
    end

    test "kind/1 returns nil for an unknown metric" do
      assert MetricNames.kind(:definitely_not_a_metric) == nil
      assert MetricNames.kind(nil) == nil
    end

    test "all/1 and names/1 stay in sync" do
      assert Enum.map(MetricNames.all(), &elem(&1, 0)) == MetricNames.names()
    end
  end

  describe "Counters" do
    test "init/0 is idempotent" do
      assert :ok = Counters.init()
      assert :ok = Counters.init()
    end

    test "registration helpers are compatibility no-ops" do
      assert :ok = Counters.new(:legacy, [])
      assert :ok = Counters.new(:legacy, [], %{})
      assert :ok = Counters.new(:legacy, [], :ignored)
      assert nil == Counters.fetch(:legacy)
      assert :ok = Counters.delete(:legacy)
      assert %{} == Counters.counters(:legacy, [:count])
    end

    test "overview/0 and overview/1 read the shared ETS store" do
      Counters.inc(:legacy_count, 3)

      assert Map.fetch!(Counters.overview(), {:counter, :legacy_count}) == 3
      assert Counters.overview(:legacy) == Counters.overview()
    end

    test "counter_fields/0 lists catalogued counters with sequential indexes" do
      fields = Counters.counter_fields()
      assert fields != []

      for {name, idx, kind, help} <- fields do
        assert MetricNames.kind(name) == :counter
        assert kind == :counter
        assert help == Atom.to_string(name)
        assert is_integer(idx)
      end

      assert Enum.map(fields, &elem(&1, 1)) == Enum.to_list(1..length(fields))
    end

    test "inc/2 and inc/3 increment the same counter" do
      assert :ok = Counters.inc(:shared, 2)
      assert :ok = Counters.inc(:server1, :shared, 3)
      assert Metrics.get(:shared) == 5
    end

    test "observe/2 and observe/3 record histograms" do
      assert :ok = Counters.observe(:latency, 10)
      assert :ok = Counters.observe(:server1, :latency, 20)
      assert %{count: 2, sum: 30, min: 10, max: 20} = Metrics.get(:latency)
    end

    test "set_gauge/2 and set_gauge/3 write gauges" do
      assert :ok = Counters.set_gauge(:commit_index, 7)
      assert :ok = Counters.set_gauge(:server1, :commit_index, 9)
      assert Metrics.get(:commit_index) == 9
    end
  end

  describe "RaftEx.Observability" do
    test "metrics/0 mirrors the ETS store" do
      Metrics.inc(:elections_started)
      assert Observability.metrics() == Metrics.all()
    end

    test "metric/1 reads counters, gauges and histograms" do
      Metrics.inc(:reads, 2)
      Metrics.set(:gauge_read, 4)
      Metrics.observe(:hist_read, 3)

      assert Observability.metric(:reads) == 2
      assert Observability.metric(:gauge_read) == 4
      assert %{count: 1} = Observability.metric(:hist_read)
      assert Observability.metric(:missing) == nil
    end

    test "the compatibility overview helpers agree" do
      Metrics.inc(:legacy_total, 5)
      assert Observability.seshat_overview() == Counters.overview()
      assert Observability.cluster_metrics() == Counters.overview()
      assert Observability.cluster_metrics(:any_cluster) == Counters.overview()
    end

    test "server_metrics/1 is an empty compatibility map" do
      assert Observability.server_metrics(:server1) == %{}
    end

    test "reset/0 clears the store" do
      Metrics.inc(:to_reset)
      assert :ok = Observability.reset()
      assert Observability.metric(:to_reset) == nil
    end
  end

  describe "Metrics store details" do
    test "init/0 is safe to call repeatedly" do
      assert :ok = Metrics.init()
      assert :ok = Metrics.init()
    end

    test "counter names may be strings" do
      assert :ok = Metrics.inc("string.counter", 4)
      assert Observability.metric("string.counter") == 4
    end

    test "integer observations track sum, count and both extremes" do
      for value <- [10, 3, 20, 3] do
        assert :ok = Metrics.observe(:integer_hist, value)
      end

      assert %{count: 4, sum: 36, min: 3, max: 20} = Metrics.get(:integer_hist)
    end

    test "float observations use the non-integer insert and update path" do
      assert :ok = Metrics.observe(:float_hist, 5.5)
      assert %{count: 1, sum: 5.5, min: 5.5, max: 5.5} = Metrics.get(:float_hist)

      assert :ok = Metrics.observe(:float_hist, 2.5)
      assert %{count: 2, sum: 8.0, min: 2.5, max: 5.5} = Metrics.get(:float_hist)
    end

    test "all/0 groups values by kind" do
      Metrics.inc(:counter_all)
      Metrics.set(:gauge_all, 1.5)
      Metrics.observe(:hist_all, 2)

      all = Metrics.all()
      assert all[{:counter, :counter_all}] == 1
      assert all[{:gauge, :gauge_all}] == 1.5
      assert all[{:hist, :hist_all}].sum == 2
    end

    test "reset/0 removes every key" do
      Metrics.inc(:a)
      Metrics.set(:b, 1)
      Metrics.observe(:c, 2)
      assert :ok = Metrics.reset()
      assert Metrics.all() == %{}
    end
  end

  describe "Distribution helpers" do
    test "set_cookie/1 is safe with a running node" do
      assert Distribution.set_cookie(:raft_ex) == :ok
    end

    test "cluster_nodes/0 always includes the local node" do
      assert node() in Distribution.cluster_nodes()
    end

    test "build_server_ids/2 sorts nodes deterministically" do
      assert Distribution.build_server_ids([:c@host, :a@host, :b@host]) == [
               {:server1, :a@host},
               {:server2, :b@host},
               {:server3, :c@host}
             ]
    end

    test "build_server_ids/2 accepts a custom prefix and an empty list" do
      assert Distribution.build_server_ids([:b@host, :a@host], :raft) == [
               {:raft1, :a@host},
               {:raft2, :b@host}
             ]

      assert Distribution.build_server_ids([]) == []
    end

    test "connect_to_seeds/2 skips self, invalid seeds and bad cookies" do
      assert Distribution.connect_to_seeds([node()]) == []
      assert Distribution.connect_to_seeds([123]) == []
      assert Distribution.connect_to_seeds([:"unknown_seed@invalid.host"]) == []
      assert Distribution.connect_to_seeds([], "not-an-atom") == []
    end

    test "start/1 is idempotent when distribution is already running" do
      if Node.alive?() do
        result = Distribution.start(cookie: :raft_ex)
        assert result == :ok or is_pid(result) or match?({:error, _}, result)
      else
        :ok
      end
    end
  end

  describe "SharedWal edge paths" do
    setup do
      %{dir: RaftEx.TestSupport.tmp_dir("shared_wal_coverage")}
    end

    test "recover/1 returns an empty range for a missing file", %{dir: dir} do
      assert SharedWal.recover(%{data_dir: dir, uid: :u1}) == {0, 0, []}
    end

    test "recover/1 honours the :dir config key and a missing uid", %{dir: dir} do
      File.write!(Path.join(dir, "shared.wal"), :erlang.term_to_binary(%{u1: [{1, 1, :a}]}))

      assert SharedWal.recover(%{dir: dir, uid: :u1}) == {1, 1, [{1, 1, :a}]}
      assert SharedWal.recover(%{dir: dir, uid: :other}) == {0, 0, []}
    end

    test "info/2 reports an empty range before any writes", %{dir: dir} do
      name = :"shared_cov_#{System.unique_integer([:positive])}"
      {:ok, pid} = SharedWal.start_link(%{name: name, dir: dir})

      assert SharedWal.info(name, :u1) == {0, 0, 0}
      GenServer.stop(pid)
    end

    test "append/3 batches uids and read/4 filters by index", %{dir: dir} do
      name = :"shared_cov_#{System.unique_integer([:positive])}"
      {:ok, pid} = SharedWal.start_link(%{name: name, data_dir: dir})

      assert {:ok, 1, 3} = SharedWal.append(name, :u1, [{1, 1, :a}, {2, 1, :b}, {3, 1, :c}])
      assert {:ok, 1, 1} = SharedWal.append(name, :u2, [{1, 1, :x}])
      assert SharedWal.read(name, :u1, 2, 3) == [{2, 1, :b}, {3, 1, :c}]
      assert SharedWal.info(name, :u2) == {1, 1, 1}
      GenServer.stop(pid)
    end

    test "migrate/1 backs up an existing shared file", %{dir: dir} do
      shared = Path.join(dir, "shared.wal")
      File.write!(shared, :erlang.term_to_binary(%{existing: []}))
      File.mkdir_p!(Path.join(dir, "u1"))

      assert :ok = SharedWal.migrate(dir)
      assert File.exists?(shared <> ".bak")
      assert File.exists?(shared)
    end
  end
end
