(ns overseer.core
  (:require [datomic.api :as d]
            [clojure.set :as set]))

(defn job-assertion [job-type tx]
  (merge
    {:db/id (d/tempid :db.part/user)
     :job/id (str (d/squuid))
     :job/status :unstarted
     :job/type job-type}
    tx))

(defn job-assertions-by-type
  "Construct a map of {:job-type => assertion} with optional
   txn data merged onto each assertion"
  [job-types tx]
  (zipmap
    (map identity job-types)
    (map #(job-assertion %) job-types)))

(defn job-dep-edges [graph jobs-by-type]
  (for [[job deps] graph
        dep deps]
    [:db/add
     (get-in jobs-by-type [job :db/id])
     :job/dep
     (get-in jobs-by-type [dep :db/id])]))

(defn ->graph-txn
  "Given a job graph, and optional additional transaction data,
   return a full transaction for all nodes

   Follow the strategy of:
     1. Assert all of the job nodes, one per type as described in
        the job graph.
     2. Assert off the dependency edges between the jobs nodes."
  ([graph]
   (->graph-txn graph {}))
  ([graph tx]
   (let [job-types (keys graph)
         jobs-by-type (job-assertions-by-type job-types tx)
         dep-edges (job-dep-edges graph jobs-by-type)]
     (concat
       (vals jobs-by-type)
       dep-edges))))

(defn ->job-entity [db job-id]
 (d/pull db '[:*] [:job/id job-id]))

(defn entity-id [db job-id]
  (:db/id (d/entity db [:job/id job-id])))

(defn status-txn [status job-id]
  (vector :db/add [:job/id job-id] :job/status status))

(defn reserve
  "Return a transaction clause that reserves the given job,
   or throws an exception.

   Requires the overseer.schema/reserve-job database function to be in-schema."
  [conn job-ent-id]
  (d/transact conn [[:reserve-job job-ent-id]]))

(defn ent-dependents
  "Find all jobs that depend on ent"
  [db ent]
  (->> (d/q '[:find ?dep
              :in $ ?ent
              :where [?dep :job/dep ?ent]]
            db
            ent)
       (map first)
       (into #{})))

(defn transitive-dependents'
  "Returns a set of job entity IDs that transitively depend upon given job entity ID
   Basically recursive breadth-first graph traversal of the job graph."
  [db job-ent-id]
  {:pre [job-ent-id]}
  (loop [all-dependents #{}
         visited #{}
         to-visit #{job-ent-id}]
    (if (empty? to-visit)
      all-dependents
      (let [ent (first to-visit)
            dependents (ent-dependents db ent)
            all-dependents' (set/union all-dependents dependents)
            visited' (conj visited ent)
            to-visit' (set/difference
                        (set/union to-visit dependents)
                        visited')]
        (recur all-dependents' visited' to-visit')))))

(defn transitive-dependents [db job-id]
  (->> (entity-id db job-id)
       (transitive-dependents' db)))
