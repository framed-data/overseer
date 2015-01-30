(ns overseer.api
  "User-facing core API"
  (:require [datomic.api :as d]
            [clojure.set :as set]
            (overseer
              [core :as core]
              [system :as system])))

(def start
  "Alias in order to start the system as a library"
  system/start)

(defn ->graph-txn
  "Entry point to add assert a sequence of jobs into the system.
   Given a job graph, and optional additional transaction data,
   return a full transaction for all nodes.

   Follow the strategy of:
     1. Assert all of the job nodes, one per type as described in
        the job graph.
     2. Assert off the dependency edges between the jobs nodes."
  ([graph]
   (->graph-txn graph {}))
  ([graph tx]
   (let [job-types (keys graph)
         jobs-by-type (core/job-assertions-by-type job-types tx)
         dep-edges (core/job-dep-edges graph jobs-by-type)]
     (concat
       (vals jobs-by-type)
       dep-edges))))

(defn fail
  ([] (fail ""))
  ([msg]
    (throw (ex-info msg {:overseer/status :failed}))))

(defn abort
  ([] (abort ""))
  ([msg]
    (throw (ex-info msg {:overseer/status :aborted}))))

(def default-config
  {:datomic {:uri "datomic:free://localhost:4334/overseer"}
   :sleep-time 10000})
