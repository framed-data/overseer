(ns ^:no-doc overseer.core
  "Internal core functions"
  (:require [clojure.set :as set]
            [framed.std.core :as std]
            [miner.herbert :as h]
            (loom derived graph)))

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

(defn job-graph
  "Given a map in Loom adjacency list format specifying dependencies between keyword job types,
  and an optional map of additional job data, return a Loom digraph of Job maps that can be
  transacted by a store. This assumes you are only generating one job per type.

  Ex:
    (def graph (job-graph
                {:start []
                 :process-1a [:start]
                 :process-1b [:process-1a]
                 :process-2 [:start]
                 :finish [:process-1b :process-2]}
                {:org/id 123}))
    ; => Graph<Job>, ex:
    ;    {{:job/id 1 :job/type :start :job/args {:org/id 123}} []
          {:job/id 2 :job/type :process-1a :job/args {:org/id 123}} [{:job/id 1 ...}]
          {:job/id 3 :job/type :process-1b :job/args {:org/id 123}} [{:job/id 2 ...}]
          ...}"
  [job-type-graph job-args]
  (let [graph (loom.graph/digraph job-type-graph)

        job-ids-by-job-type ; {:job-type-foo "abc123..."}
        (std/zipmap-seq identity (fn [_] (str (squuid))) (loom.graph/nodes graph))

        job-type->job-map
        (fn [job-type]
          {:job/id (job-ids-by-job-type job-type)
           :job/status :unstarted
           :job/type job-type
           :job/args job-args})]
    (loom.derived/mapped-by job-type->job-map graph)))

(defn missing-handlers
  "Given a map of {job-type job-handler} and an adjacency list map specifying
  dependencies between job types, return the set of handlers that are referenced
  in the graph but not specified in `handlers`"
  [handlers job-type-graph]
  (-> (loom.graph/nodes (loom.graph/digraph job-type-graph))
      (set/difference (keys handlers))))

(defn heartbeat
  "Generate a Job heartbeat for the current time"
  []
  (quot (System/currentTimeMillis) 1000))

; Core functions encompassing the system's interaction with storage backends
; These functions effectively represent the finite state machine transitions
; that a job goes through:
;
;      unstarted ---------+
;       |  ^              |
;       |  |              |
;       v  |              |
;      started -----------+
;      /     \            |
;     /        \          |
; finished   failed   aborted
;
; The system internally handles the transitions:
;   unstarted -> started
;   started -> finished
;   started -> failed
;
; Callers can force transitions using the functions in `overseer.api`:
;   started -> unstarted
;   started,unstarted -> aborted (affects dependent children as well)
(defprotocol Store
  (install [this]
    "Install store configuration (create system tables, etc). *Not* guaranteed
    to be idempotent. Returns :ok on success")

  (transact-graph [this graph]
    "Given a Graph, atomically transact all of its jobs/dependencies into the store and
    return it. Idempotent on job-ids in graph: if any job-ids already exist in store, will not
    double insert *or* update (if different args supplied, for example)")

  (job-info [this job-id]
    "Given a job-id, return a Job")

  (reserve-job [this job-id]
    "Reserve the given job-id, i.e. set its :job/status to :started.
    Returns Job, or nil if unable to reserve (lost race against other worker?).")

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
    Returns nil if not :started (lost race against other monitor?). Raises if update fails.")

  (jobs-ready [this]
    "Return a seq of job-ids that are ready to run.
    Implementations may choose to bound the max size of this set.")

  (jobs-dead [this threshold]
    "Return a seq of job-ids whose heartbeats are older than `threshold`,
    where `threshold` is an integer UNIX timestamp.
    Implementations may choose to bound the max size of this set."))
