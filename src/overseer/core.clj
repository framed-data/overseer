(ns ^:no-doc overseer.core
  "Internal core functions"
  (:require [datomic.api :as d]
            [clojure.set :as set]))

(defn missing-dependencies
  "Compute dependencies that have been referenced but not
   specified in a graph, if any"
  [graph]
  (->> (for [[k deps] graph
             d deps]
         (when-not (get graph d) d))
       (filter identity)))

(defn missing-handlers [handlers graph]
  (->> (filter (fn [[k _]] (not (contains? handlers k))) graph)
       (map first)))

(defn job-txn
  ([job-type]
   (job-txn job-type {}))
  ([job-type tx]
   (merge
     {:db/id (d/tempid :db.part/user)
      :job/id (str (d/squuid))
      :job/status :unstarted
      :job/type job-type}
     tx)))

(defn job-txns-by-type
  "Construct a map of {:job-type => txn} with optional
   txn data merged onto each txn"
  [job-types tx]
  (zipmap
    (map identity job-types)
    (map #(job-txn % tx) job-types)))

(defn job-dep-edges
  "Construct a list of txns to of the graph edges, i.e marking
   job dependencies"
  [graph jobs-by-type]
  (for [[job deps] graph
        dep deps]
    {:db/id (get-in jobs-by-type [job :db/id])
     :job/dep (get-in jobs-by-type [dep :db/id])}))

(defn ->job-entity [db job-id]
  {:pre [job-id]}
  (d/pull db '[:*] [:job/id job-id]))

(defn ent-dependents
  "Find all jobs entities that depend on ent"
  [db ent]
  (->> (d/q '[:find ?dep
              :in $ ?ent
              :where [?dep :job/dep ?ent]]
            db
            ent)
       (map first)
       (set)))

(defn transitive-dependents [db job-id]
  "Returns a set of job IDs that transitively depend upon given job ID
   Basically recursive breadth-first graph traversal of the job graph."
  (let [job-ent-id (:db/id (->job-entity db job-id))]
    (assert job-ent-id)
    (loop [all-dependents #{}
           visited #{}
           to-visit #{job-ent-id}]
      (if (empty? to-visit)
        (set (map #(:job/id (d/entity db %)) all-dependents))
        (let [ent (first to-visit)
              dependents (ent-dependents db ent)
              all-dependents' (set/union all-dependents dependents)
              visited' (conj visited ent)
              to-visit' (set/difference
                          (set/union to-visit dependents)
                          visited')]
          (recur all-dependents' visited' to-visit'))))))

(defn status-txn
  "Construct a single job update status txn"
  [{:keys [overseer/status overseer/failure]} job-id]
  (let [base-txn {:db/id [:job/id job-id]
                  :job/status status}]
    (if failure
      (assoc base-txn :job/failure (pr-str failure))
      base-txn)))

(defn update-job-status-txns
  "Construct a seq of txns for updating a job's status
   If job was aborted, then also abort all its dependents"
  [db job-id {:keys [overseer/status] :as status-map}]
  (let [job-ids (if (= :aborted status)
                  (cons job-id (transitive-dependents db job-id))
                  [job-id])]
    (map (partial status-txn status-map) job-ids)))
