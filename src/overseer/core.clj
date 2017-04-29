(ns ^:no-doc overseer.core
  "Internal core functions"
  (:require (loom alg derived graph)
            [framed.std.core :as std]
            [clojure.set :as set]))

(defprotocol Store
  (job-info [this job-id])

  (reserve-job [this job-id])
  (finish-job [this job-id])
  (fail-job [this job-id failure])
  (heartbeat-job [this job-id])
  (abort-job [this job-id])
  (reset-job [this job-id])

  (jobs-ready [this])
  (jobs-dead [this thresh])

  (transact-graph [this graph]))

(defn squuid []
  (let [uuid (java.util.UUID/randomUUID)
        time (System/currentTimeMillis)
        secs (quot time 1000)
        lsb (.getLeastSignificantBits uuid)
        msb (.getMostSignificantBits uuid)
        timed-msb (bit-or (bit-shift-left secs 32)
                          (bit-and 0x00000000ffffffff msb))]
    (java.util.UUID. timed-msb lsb)))

(defn missing-handlers [handlers keyword-adjacency-list]
  (-> (loom.graph/nodes (loom.graph/digraph keyword-adjacency-list))
      (set/difference (keys handlers))))

(defn job-graph [keyword-adjacency-list tx]
  (let [keyword-graph (loom.graph/digraph keyword-adjacency-list)

        job-ids-by-job-type ; {:job-type-foo #uuid "abc123..."}
        (std/zipmap-seq identity (fn [_] (squuid)) (loom.graph/nodes keyword-graph))

        job-type->job-map
        (fn [job-type]
          (merge
            {:job/id (job-ids-by-job-type job-type)
             :job/status :unstarted
             :job/type job-type}
            tx))]
    (loom.derived/mapped-by job-type->job-map keyword-graph)))

(defn ready-job-info
  "Return the set of job entities that are ready to execute,
   filtering to those defined in job-handlers (this allows different
   nodes to solely execute certain types of jobs, if desired)"
  [store job-handlers]
  (->> (jobs-ready store)
       (map (partial job-info store))
       (filter (comp job-handlers :job/type))
       set))

(defn simple-graph [& jobs]
  (apply loom.graph/add-nodes (loom.graph/digraph) jobs))
