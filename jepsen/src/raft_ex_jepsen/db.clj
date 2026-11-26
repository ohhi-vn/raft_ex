(ns raft-ex-jepsen.db
  "Database lifecycle for RaftEx nodes under Jepsen."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [jepsen.control :as c]))

(defn raft-ex-db
  "A Jepsen db that starts/stops an Elixir RaftEx node on each test node."
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (info node "Starting RaftEx on" node)
      (c/su (c/exec :mkdir :-p "/var/lib/raft_ex"))
      ;; Assume Elixir/Erlang and the raft_ex release are installed on the node.
      (c/su (c/exec :sh :-c "cd /opt/raft_ex && nohup bin/raft_ex daemon > /tmp/raft_ex.log 2>&1 &"))
      (Thread/sleep 3000))

    (teardown! [_ test node]
      (info node "Stopping RaftEx on" node)
      (c/su (c/exec :pkill :-f "raft_ex" :|| true)))

    ))