defmodule RaftEx.Infra.Persistence.SnapshotTest do
  use ExUnit.Case, async: true

  alias RaftEx.Infra.Persistence.{LogSnapshot, Snapshot}

  test "LogSnapshot implements the Snapshot behaviour" do
    assert LogSnapshot.__info__(:attributes)[:behaviour] == [Snapshot]
  end

  test "all required callbacks are exported" do
    exports = LogSnapshot.__info__(:functions)

    for {name, arity} <- [
          prepare: 2,
          write: 4,
          sync: 1,
          begin_read: 2,
          read_chunk: 3,
          begin_accept: 2,
          accept_chunk: 2,
          complete_accept: 2,
          recover: 1,
          validate: 1,
          read_meta: 1
        ] do
      assert {name, arity} in exports
    end
  end
end
