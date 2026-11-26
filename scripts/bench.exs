# Run with: mix run scripts/bench.exs [options]
#
# See `mix run scripts/bench.exs --help` for the full option list. The
# scenario modules live under bench/ and are loaded explicitly so the
# benchmark suite stays out of the compiled application.

root = Path.expand("..", __DIR__)

Code.require_file(Path.join(root, "bench/json.ex"))
Code.require_file(Path.join(root, "bench/scenarios.ex"))
Code.require_file(Path.join(root, "bench/raft_ex_bench.ex"))

RaftExBench.main(System.argv())
