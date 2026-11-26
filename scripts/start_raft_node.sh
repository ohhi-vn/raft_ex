#!/usr/bin/env bash
# Start a named RaftEx node for multi-host testing.
#
# Usage:
#   scripts/start_raft_node.sh <node_name> <cookie> <data_dir> [port]
#
# Example:
#   scripts/start_raft_node.sh raft@host1 secret /var/lib/raft_ex 4369

set -euo pipefail

NODE_NAME="${1:?node name required (e.g. raft@host1)}"
COOKIE="${2:?cookie required}"
DATA_DIR="${3:?data dir required}"
PORT="${4:-4369}"

mkdir -p "$DATA_DIR"

ERL_AFLAGS="-kernel inet_dist_listen_min $PORT inet_dist_listen_max $PORT" \
exec elixir --name "$NODE_NAME" --cookie "$COOKIE" -S mix run --no-halt \
  -e "RaftEx.start_in(\"$DATA_DIR\"); IO.puts(\"RaftEx started on #{node()}\")"
