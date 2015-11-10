(ns overseer.test-utils
  (:require [datomic.api :as d]
            [framed.std.core :as std]
            [overseer.schema :as schema]))

(defn bootstrap-db-uri
  "Create/bootstrap a fresh memory DB and return its uri"
  []
  (let [uri (str "datomic:mem://" (std/rand-alphanumeric 32))]
    (d/create-database uri)
    (schema/install (d/connect uri))
    uri))

(defn connect []
  (d/connect (bootstrap-db-uri)))

(defn ->transact-job
  "Helper to transact an unstarted job and return it,
   optionally accepting attributes for the job"
  ([conn] (->transact-job conn {}))
  ([conn job-data]
    (let [job-tempid (d/tempid :db.part/user -1000)
          job-txn
          (merge {:db/id job-tempid
                  :job/id (str (d/squuid))
                  :job/status :unstarted}
                 job-data)
          {:keys [tempids]} @(d/transact conn [job-txn])
          job-ent-id (d/resolve-tempid (d/db conn) tempids job-tempid)]
      (assoc job-txn :db/id job-ent-id))))
