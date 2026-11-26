(defproject raft-ex-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for RaftEx"
  :url "https://github.com/manhvu/raft_ex"
  :license {:name "MPL-2.0"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.6"]
                 [knossos "0.3.10"]]
  :main raft-ex-jepsen.core
  :aot [raft-ex-jepsen.core])