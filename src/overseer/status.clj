(ns overseer.status
  "Functions for querying the state of jobs in the system"
  (:require [datomic.api :as d]
            [clojure.set :as set]))

(defn jobs-with-status
  "Find all job IDs with a given status (e.g. :unstarted)"
  [db status]
  (->> (d/q '[:find ?jid
              :in $ ?status
              :where [?e :job/status ?status]
                     [?e :job/id ?jid]]
            db
            status)
       (map first)
       (set)))

(defn jobs-unfinished
  "Find all job IDs that are not yet complete."
  [db]
  (->> (d/q '[:find ?jid
              :where [?e :job/status ?s]
                     [((comp not contains?) #{:finished :aborted :failed} ?s)]
                     [?e :job/id ?jid]]
            db)
       (map first)
       (into #{})))

(defn jobs-ready
  "Find all job IDs that are ready to run.
   Works by finding all jobs that are not yet done, and subtracting the
   jobs who dependencies are not yet ready."
  [db]
  (let [unfinished (jobs-unfinished db)]
    (set/difference
      unfinished
      (->> (d/q '[:find ?jid
                  :in $ [?unfinished-jids ...]
                  :where [?j :job/id ?unfinished-jids]
                         [?j :job/id ?jid]
                         [?j :job/dep ?dj]
                         [?dj :job/status ?djs]
                         [(not= :finished ?djs)]]
                db
                unfinished)
           (map first)
           (into #{})))))
