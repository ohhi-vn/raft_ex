Application.put_env(:raft_ex, :invariant_checks, :test)
ExUnit.start(exclude: [:distributed, :multi_host, :faults])
