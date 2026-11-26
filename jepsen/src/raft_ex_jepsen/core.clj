(ns raft-ex-jepsen.core
  "Entry point for RaftEx Jepsen tests."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.core :as jepsen]
            [jepsen.nemesis :as nemesis]
            [jepsen.cli :as cli]
            [jepsen.tests :as tests]
            [raft-ex-jepsen.db :as db]
            [raft-ex-jepsen.workload :as workload]))

(defn gen-ops []
  (concat (map (fn [i] {:type :invoke, :f :write, :value i}) (range 100))
          (repeat {:type :invoke, :f :read, :value nil})))

(defn raft-ex-test
  "A Jepsen test for RaftEx."
  [opts]
  (let [w (workload/workload opts)]
    (merge tests/noop-test
           {:name "raft-ex"
            :os :debian
            :db (db/raft-ex-db opts)
            :client (:client w)
            :checker (:checker w)
            :model (:model w)
            :nemesis (nemesis/partition-random-halves)
            :generator (gen-ops)
            :nodes (:nodes opts)
            :time-limit (or (:time-limit opts) 60)})))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn (fn [opts] (raft-ex-test opts))}) args))