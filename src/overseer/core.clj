(ns ^:no-doc overseer.core
  "Internal core functions"
  (:require [datomic.api :as d]
            [clojure.set :as set]
            [miner.herbert :as h]
            loom.graph))

(def job-schema
  "Jobs are defined as maps with the requisite descriptive keys/values."
  '{:job/id str
    :job/type kw
    :job/status (or :unstarted :started :finished :failed :aborted)
    :job/heartbeat? int
    :job/args? map
    :job/failure? map})

(defn valid-job? [job]
  (h/conforms? job-schema job))

(defn valid-graph?
  "Graphs are defined as Loom digraphs graphs where every node is a valid Job.
  See `loom.graph/digraph`."
  [graph]
  (and (satisfies? loom.graph/Digraph graph)
       (every? valid-job? (loom.graph/nodes graph))))

(defprotocol Store
  (job-info [this job-id]
    "Given a job-id, return a Job")

  (transact-graph [this graph]
    "Given a Graph, atomically transact all of its jobs/dependencies into the store.
    Side-effecting only; return value is undefined (raises if transaction fails)")

  (reserve-job [this job-id]
    "Reserve the given job-id, i.e. set its :job/status to :started.
    Returns Job, or nil if unable to reserve.")

  (finish-job [this job-id]
    "Finish the given job-id, i.e. set its :job/status to :finished.
    Side-effecting only; return value is undefined (raises if update fails).")

  (fail-job [this job-id failure]
    "Fail the given job-id, i.e. set its :job/status to :failed and set
    its :job/failure to (pr-str failure) if present.
    Side-effecting only; return value is undefined (raises if update fails).")

  (heartbeat-job [this job-id]
    "Update the last heartbeat time of a job to the current time.
    Side-effecting only; return value is undefined (raises if update fails).")

  (abort-job [this job-id]
    "Abort the given job-id and any jobs that depend upon it.
    Side-effecting only; return value is undefined (raises if update fails).")

  (reset-job [this job-id]
    "Reset the given job-id, i.e. set its :job/status to :unstarted.
    Returns nil if not :started (raises if update fails).")

  (jobs-ready [this]
    "Return a seq of job-ids that are ready to run.
    Implementations may choose to bound the max size of this set.")

  (jobs-dead [this threshold]
    "Return a seq of job-ids whose heartbeats are older than `threshold`,
    where `threshold` is an integer UNIX timestamp.
    Implementations may choose to bound the max size of this set."))

(defn squuid
  "Sequential UUID, which can have favorable index performance when inserted into
  a DB at scale since they generate in linear order, not randomly.

  From the Clojure Cookbook: https://github.com/clojure-cookbook"
  []
  (let [uuid (java.util.UUID/randomUUID)
        secs (quot (System/currentTimeMillis) 1000)
        lsb (.getLeastSignificantBits uuid)
        msb (.getMostSignificantBits uuid)
        timed-msb (bit-or (bit-shift-left secs 32)
                          (bit-and 0x00000000ffffffff msb))]
    (java.util.UUID. timed-msb lsb)))

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

(defn transitive-dependents [db job-id]
  "Returns a set of job IDs that transitively depend upon given job ID
  Basically recursive breadth-first graph traversal of the job graph."
  (let [rules '[[(dependent? ?j1 ?j0)
                 [?j1 :job/dep ?j0]]
                [(dependent? ?j2 ?j0)
                 (dependent? ?j2 ?j1)
                 (dependent? ?j1 ?j0)]]]
    (set (d/q '[:find [?dep-jid ...]
                :in $ % ?jid
                :where [?j0 :job/id ?jid]
                       [dependent? ?j1 ?j0]
                       [?j1 :job/id ?dep-jid]]
              db
              rules
              job-id))))

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
