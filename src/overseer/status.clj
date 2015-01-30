(ns overseer.status
  "Functions for querying the state of jobs in the system"
  (:require [datomic.api :as d]
            [clojure.set :as set]))

(defn jobs-with-status [db status]
  (->> (d/q '[:find ?jid
              :in $ ?status
              :where
              [?e :job/status ?status]
              [?e :job/id ?jid]]
            db
            status)
       (map first)
       (set)))

(defn jobs-unfinished
  "Find all jobs that are not yet complete."
  [db]
  (->> (d/q '[:find ?jid
              :where
              [?e :job/status ?s]
              [((comp not contains?) #{:finished :aborted :failed} ?s)]
              [?e :job/id ?jid]]
            db)
       (map first)
       (into #{})))

(defn jobs-ready
  "Find all jobs that are ready to run.
   Works by finding all jobs that are not yet done, and subtracting the
   jobs who dependencies are not yet ready."
  [db]
  (set/difference
    (jobs-unfinished db)
    (->> (d/q '[:find ?jid
                :where
                [?j :job/dep ?dj]
                [?dj :job/status ?djs]
                [(not= :finished ?djs)]
                [?j :job/id ?jid]]
              db)
         (map first)
         (into #{}))))
