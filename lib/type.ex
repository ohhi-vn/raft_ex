defmodule RaftEx.Types do
  @moduledoc """
  Shared type definitions and protocol message structs for the Raft implementation.

  All Raft wire-protocol messages (RPCs, replies) are defined here as structs
  so they can be pattern-matched cleanly across the codebase.
  """

  @type index :: non_neg_integer()
  @type term_num :: non_neg_integer()
  @type idxterm :: {index(), term_num()}
  @type server_id :: {name :: atom(), node :: node()}
  @type cluster_name :: binary() | atom()
  @type uid :: binary()
  @type membership :: :voter | :promotable | :non_voter | :unknown

  @type ra_state ::
          :leader
          | :follower
          | :candidate
          | :pre_vote
          | :await_condition
          | :delete_and_terminate
          | :terminating_leader
          | :terminating_follower
          | :recover
          | :recovered
          | :stop
          | :receive_snapshot

  @type peer_status ::
          :normal
          | {:sending_snapshot, pid(), non_neg_integer()}
          | {:snapshot_backoff, non_neg_integer()}
          | :suspended
          | :disconnected

  @type voter_status :: %{
          optional(:membership) => membership(),
          optional(:uid) => uid(),
          optional(:target) => index()
        }

  @type peer_state :: %{
          optional(:voter_status) => voter_status(),
          optional(:machine_version) => non_neg_integer(),
          next_index: non_neg_integer(),
          match_index: non_neg_integer(),
          query_index: non_neg_integer(),
          commit_index_sent: non_neg_integer(),
          status: peer_status()
        }

  @type cluster :: %{server_id() => peer_state()}
  @type log_entry :: {index(), term_num(), term()}

  # ---------------------------------------------------------------------------
  # Wire-protocol message structs
  # ---------------------------------------------------------------------------

  defmodule AppendEntriesRpc do
    @moduledoc "Leader → follower: replicate log entries (or heartbeat when entries: [])."
    defstruct [:term, :leader_id, :leader_commit, :prev_log_index, :prev_log_term, entries: []]
  end

  defmodule AppendEntriesReply do
    @moduledoc "Follower → leader: response to an AppendEntries RPC."
    defstruct [:term, :success, :next_index, :last_index, :last_term]
  end

  defmodule RequestVoteRpc do
    @moduledoc "Candidate → peers: request a vote in an election."
    defstruct [:term, :candidate_id, :last_log_index, :last_log_term]
  end

  defmodule RequestVoteResult do
    @moduledoc "Peer → candidate: vote grant/deny."
    defstruct [:term, :vote_granted]
  end

  defmodule PreVoteRpc do
    @moduledoc "Pre-vote probe to check whether an election would succeed."
    defstruct [
      :machine_version,
      :term,
      :token,
      :candidate_id,
      :last_log_index,
      :last_log_term,
      version: 1
    ]
  end

  defmodule PreVoteResult do
    @moduledoc "Peer → pre-vote candidate: pre-vote result."
    defstruct [:term, :token, :vote_granted]
  end

  defmodule InstallSnapshotRpc do
    @moduledoc "Leader → follower: transfer a snapshot."
    defstruct [:term, :leader_id, :meta, :chunk_state, :data]
  end

  defmodule InstallSnapshotResult do
    @moduledoc "Follower → leader: snapshot installation result."
    defstruct [:term, :last_index, :last_term]
  end

  defmodule HeartbeatRpc do
    @moduledoc "Lightweight heartbeat used for consistent query acknowledgement."
    defstruct [:query_index, :term, :leader_id]
  end

  defmodule HeartbeatReply do
    @moduledoc "Follower response to a HeartbeatRpc."
    defstruct [:query_index, :term]
  end

  defmodule InfoRpc do
    @moduledoc "Generic info request between peers."
    defstruct [:from, :term, :keys]
  end

  defmodule InfoReply do
    @moduledoc "Response to an InfoRpc."
    defstruct [:from, :term, :keys, info: %{}]
  end
end
