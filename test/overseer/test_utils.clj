(ns overseer.test-utils
  "Helper functions for test suite"
  (:require (clj-time
              [core :as tcore]
              [coerce :as tcoerce])
            [datomic.api :as d]
            [overseer.schema :as schema]))

(def test-config
  {:datomic {:uri "datomic:mem://overseer_test"}})

(def test-datomic-uri (get-in test-config [:datomic :uri]))

(defn connect []
  (d/connect test-datomic-uri))

(defn refresh-database [datomic-uri]
  (d/delete-database datomic-uri)
  (d/create-database datomic-uri)
  (schema/install (d/connect datomic-uri)))

(defn setup-db-fixtures [f]
  (refresh-database test-datomic-uri)
  (f))

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

(defn inst [y m d h mm s]
  (tcoerce/to-date (tcore/date-time y m d h mm s)))
