(ns ^:no-doc overseer.store.datomic
  "Implementation of overseer.core/Store for Datomic"
  (:require [clojure.set :as set]
            [datomic.api :as d]
            [framed.std.core :as std]
            (loom derived graph)
            (overseer
              [core :as core]
              [util :as util]))
  (:import java.util.concurrent.ExecutionException))

(def schema-txn
  "Datomic transaction to install the store"
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
    :db/ident :job/args
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "The arguments passed to a job (serialized as EDN)"
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

(def abort-if-job-exists
  "DB function to abort a transaction if any job-id in `job-ids` already exists in the DB"
  {:db/id (d/tempid :db.part/user)
   :db/ident :abort-if-job-exists
   :db/fn
   (datomic.function/construct
     {:lang "clojure"
      :params '[db job-ids]
      :code
      '(when-let [existing-ent (some #(datomic.api/entity db [:job/id %]) job-ids)]
         (throw (ex-info (format "Job exists in DB: %s" (pr-str existing-ent))
                         {:cause :job-id-exists})))})})

(defn install' [conn]
  @(d/transact conn (conj schema-txn abort-if-job-exists))
  :ok)

(defn job-info' [db job-id]
  (-> (d/pull db [:*] [:job/id job-id])
      (util/when-update :job/args std/from-edn)
      (util/when-update :job/failure std/from-edn)))

(defn dependents [db job-id]
  "Returns the set of all Job IDs that depend on given `job-id`, both
  directly and transitively"
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

(defn jobs-ready'
  "Find all job IDs that are ready to run, i.e. :unstarted
  and not blocked (i.e. dependent on an unfinished job)"
  [db]
  (let [rules '[[(blocked? ?j)
                 [?j :job/dep ?dep]
                 [?dep :job/status ?js]
                 [(not= :finished ?js)]]]]
    (->> (d/q '[:find [?jid ...]
                :in $ %
                :where [?j :job/status :unstarted]
                       (not [blocked? ?j])
                       [?j :job/id ?jid]]
              db
              rules)
         set)))

(defn jobs-dead'
  "Find all jobs whose heartbeats are older than `thresh` timestamp"
  [db thresh]
  (d/q '[:find [?jid ...]
         :in $ ?thresh
         :where [?e :job/status :started]
                [?e :job/id ?jid]
                [?e :job/heartbeat ?h]
                [(< ?h ?thresh)]]
       db
       thresh))

(defn loom-graph->datomic-txn
  "Given a Loom job graph, return a Datomic transaction representing it."
  [graph]
  (let [job-assertion
        (fn [job]
          (-> job
              (assoc :db/id (d/tempid :db.part/user))
              (util/when-update :job/args std/to-edn)
              (util/when-update :job/failure std/to-edn)))

        graph-with-tempids
        (loom.derived/mapped-by job-assertion graph)

        dep-edge
        (fn [[job0 job1]]
          [:db/add (:db/id job0) :job/dep (:db/id job1)]) ]
    (concat
      (loom.graph/nodes graph-with-tempids)
      (map dep-edge (loom.graph/edges graph-with-tempids)))))

(defn cas-failed?
  "Return whether an exception is specifically a Datomic check-and-set failure"
  [ex]
  (= :db.error/cas-failed
     (->> ex
          Throwable->map
          :data
          :db/error)))

(defmacro with-ignore-cas [& body]
  `(try
    ~@body
    (catch ExecutionException ex#
      (when-not (cas-failed? ex#)
        (throw ex#)))))

(defn heartbeat-assertion [job-id]
  [:db/add [:job/id job-id] :job/heartbeat (core/heartbeat)])

(defrecord DatomicStore [conn]
  core/Store
  (install [this]
    (install' conn))

  (transact-graph [this graph]
    (assert (core/valid-graph? graph))
    (let [db (d/db conn)
          proposed-ids (->> graph loom.graph/nodes (map :job/id))
          tx (loom-graph->datomic-txn graph)]
      (try
        (->> tx
             (into [[:abort-if-job-exists proposed-ids]])
             (d/transact conn)
             deref)
        (catch ExecutionException ex
          (when-not (= :job-id-exists (->> ex (.getCause) ex-data :cause))
            (throw ex))))
      graph))

  (job-info [this job-id]
    (job-info' (d/db conn) job-id))

  (reserve-job [this job-id]
    (with-ignore-cas
      (let [{:keys [db-after]}
            @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :unstarted :started]
                               (heartbeat-assertion job-id)])]
        (job-info' db-after job-id))))

  (finish-job [this job-id]
    @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :started :finished]]))

  (fail-job [this job-id failure]
    @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :started :failed]
                       [:db/add [:job/id job-id] :job/failure (pr-str failure)]]))

  (heartbeat-job [this job-id]
    @(d/transact conn [(heartbeat-assertion job-id)]))

  (abort-job [this job-id]
    (let [job-ids (cons job-id (dependents (d/db conn) job-id))
          abort (fn [jid] [:db/add [:job/id jid] :job/status :aborted])]
      @(d/transact conn (map abort job-ids))))

  (reset-job [this job-id]
    (let [old-heartbeat (:job/heartbeat (d/pull (d/db conn) [:job/heartbeat] [:job/id job-id]))]
      (with-ignore-cas
        @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/heartbeat old-heartbeat (core/heartbeat)]
                           [:db.fn/cas [:job/id job-id] :job/status :started :unstarted]]))))

  (jobs-ready [this]
    (jobs-ready' (d/db conn)))

  (jobs-dead [this thresh]
    (jobs-dead' (d/db conn) thresh)))

(defn store [datomic-uri]
  (->DatomicStore (d/connect datomic-uri)))
