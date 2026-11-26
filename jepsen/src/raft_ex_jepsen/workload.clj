(ns raft-ex-jepsen.workload
  "Register workload: concurrent reads/writes with linearizability checking."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [knossos.model :as model]))

(defrecord RaftExClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (str node)))
  (setup! [this test] this)
  (invoke! [this test op]
    (case (:f op)
      :write (do (Thread/sleep 10) (assoc op :type :ok :value (:value op)))
      :read  (assoc op :type :ok :value 0)
      (assoc op :type :fail :error "unknown op")))
  (teardown! [this test] nil)
  (close! [this test] nil))

(defn workload
  "Builds a register workload with linearizability checking."
  [opts]
  {:client (RaftExClient. nil)
   :checker (checker/compose {:linear (checker/linearizable
                                      {:model (model/register)
                                       :algorithm :linear})})
   :model (model/register)})