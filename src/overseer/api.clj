(ns overseer.api
  "User-facing core API"
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [datomic.api :as d]
            (overseer
              [core :as core]
              [system :as system]
              [worker :as worker])))

(def default-config
  {:datomic {:uri "datomic:free://localhost:4334/overseer"}
   :sleep-time worker/default-sleep-time})

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
   (let [missing-deps (core/missing-dependencies graph)]
      (assert (empty? missing-deps)
              (str "Invalid graph; missing dependencies " (string/join ", " missing-deps)))
     (let [job-types (keys graph)
           jobs-by-type (core/job-assertions-by-type job-types tx)
           dep-edges (core/job-dep-edges graph jobs-by-type)]
       (concat
         (vals jobs-by-type)
         dep-edges)))))

(defn validate-graph-handlers [handlers graph]
  (let [missing-handlers (core/missing-handlers handlers graph)]
    (assert (empty? missing-handlers)
            (str "Invalid graph; missing handlers " (string/join ", " missing-handlers)))))

(defn fail
  "Control-flow helper to mark a job as failed from within a handler
   (halts handler execution)"
  ([] (fail ""))
  ([msg]
    (throw (ex-info msg {:overseer/status :failed}))))

(defn abort
  "Control-flow helper to mark a job as aborted from within a handler
   and abort all of its dependents (halts handler execution)"
  ([] (abort ""))
  ([msg]
    (throw (ex-info msg {:overseer/status :aborted}))))
