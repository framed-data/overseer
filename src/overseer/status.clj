(ns overseer.status
  "Functions for querying the state of jobs in the system"
  (:require [datomic.api :as d]
            [clojure.set :as set]))

(defn jobs-with-status
  "Find all job IDs with a given status (e.g. :unstarted)"
  [db status]
  (->> (d/q '[:find [?jid ...]
              :in $ ?status
              :where [?e :job/status ?status]
                     [?e :job/id ?jid]]
            db
            status)
       set))

(defn jobs-unstarted
  "Find all job IDs that are not yet started."
  [db]
  (->> (d/q '[:find [?jid ...]
              :where [?e :job/status :unstarted]
                     [?e :job/id ?jid]]
            db)
       set))

(defn jobs-ready
  "Find all job IDs that are ready to run.
   Works by finding all jobs that are not yet done, and subtracting the
   jobs who dependencies are not yet ready."
  [db]
  (let [unstarted (jobs-unstarted db)]
    (set/difference
      unstarted
      (->> (d/q '[:find [?jid ...]
                  :in $ [?unstarted-jids ...]
                  :where [?j :job/id ?unstarted-jids]
                         [?j :job/id ?jid]
                         [?j :job/dep ?dj]
                         [?dj :job/status ?djs]
                         [(not= :finished ?djs)]]
                db
                unstarted)
           set))))
