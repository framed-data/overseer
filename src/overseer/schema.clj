(ns overseer.schema
  (:require [datomic.api :as d]))

 (def schema-txn
   [{:db/id (d/tempid :db.part/db)
     :db/ident :job/id
     :db/valueType :db.type/string
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "A job's unique ID (a semi-sequential UUID)"
     :db.install/_attribute :db.part/db}

    {:db/id (d/tempid :db.part/db)
     :db/ident :job/args
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one
     :db/doc "The arguments passed to a job (serialized as EDN)"
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
     :db/doc "An edn serialized map containing a jobs failure information"
     :db.install/_attribute :db.part/db}

    {:db/id (d/tempid :db.part/db)
     :db/ident :job/dep
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/many
     :db/doc
     "Dependency of this job ('parent'). Refers to other jobs
     that must be completed before this job can run."
     :db.install/_attribute :db.part/db}])

(def reserve-job
  "Datomic database function to atomically reserve a job.
   Either reserves the given job id, or throws."
  {:db/id (d/tempid :db.part/user)
   :db/ident :reserve-job
   :db/fn (datomic.function/construct
            {:lang "clojure"
             :params '[db job-id]
             :code
             '(let [result (datomic.api/q '[:find ?s
                                            :in $data ?job-id
                                            :where [$data ?e :job/id ?job-id]
                                                   [$data ?e :job/status ?s]]
                             db
                             job-id)
                    status (ffirst result)]
                (if-not (#{:finished :aborted :failed} status)
                  [[:db/add [:job/id job-id] :job/status :started]]
                  (throw (ex-info (format "Job %s: status %s not eligible for start." job-id status)
                                  {:overseer/error :ineligible}))))})})

(defn install
  "Install Overseer's schema and DB functions into Datomic"
  [conn]
  @(d/transact conn (conj schema-txn reserve-job))
  :ok)
