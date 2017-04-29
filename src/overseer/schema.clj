(ns overseer.schema
  "Functions for controlling Overseer's operational data"
  (:require [datomic.api :as d]))

 (def ^:no-doc schema-txn
   [{:db/id (d/tempid :db.part/db)
     :db/ident :job/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "A job's unique ID (a semi-sequential UUID)"
     :db.install/_attribute :db.part/db}

    {:db/id (d/tempid :db.part/db)
     :db/ident :job/type
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one
     :db/doc "A job's type, represented by a keyword"
     :db.install/_attribute :db.part/db}

    {:db/id (d/tempid :db.part/db)
     :db/ident :job/status
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one
     :db/doc "A job's status (unstarted|started|aborted|failed|finished)"
     :db/index true
     :db.install/_attribute :db.part/db}

    {:db/id (d/tempid :db.part/db)
     :db/ident :job/failure
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db/doc "An EDN serialized map containing a jobs failure information"
     :db.install/_attribute :db.part/db}

    {:db/id (d/tempid :db.part/db)
     :db/ident :job/dep
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/many
     :db/doc
     "Dependency of this job ('parent'). Refers to other jobs
     that must be completed before this job can run."
     :db.install/_attribute :db.part/db}

    {:db/id (d/tempid :db.part/db)
     :db/ident :job/heartbeat
     :db/valueType :db.type/long
     :db/cardinality :db.cardinality/one
     :db/doc "Unix timestamp of periodic heartbeat from node working on this job"
     :db.install/_attribute :db.part/db}])

(defn install
  "Install Overseer's schema and DB functions into Datomic.
   Should only be necessary a single time. Returns :ok on success"
  [conn]
  @(d/transact conn (conj schema-txn))
  :ok)
