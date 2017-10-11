(ns ^:no-doc overseer.store.jdbc
  "Implementation of overseer.core/Store for JDBC"
  (:require [clojure.java.jdbc :as j]
            [clojure.set :as set]
            [clojure.string :as string]
            (clj-time
              [core :as clj-time]
              [jdbc]) ; Requiring extends necessary coercion protocols
            [framed.std.core :as std]
            [honeysql.core :as sql]
            (loom graph)
            (overseer
              [core :as core]
              [util :as util]))
  (:import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException))

(def status-code
  {:unstarted 0
   :started 1
   :finished 2
   :failed 3
   :aborted 4})

(def status
  (set/map-invert status-code))

(defn job->jdbc-map [job]
  {:pre [job]}
  (let [{:keys [job/id job/type job/args job/status
                job/failure job/heartbeat]} job]
    (-> {:id id
         :type (name type)
         :status (get status-code status)}
        (std/when-assoc :args (some-> args std/to-edn))
        (std/when-assoc :failure (some-> failure std/to-edn))
        (std/when-assoc :heartbeat heartbeat))))

(defn jdbc-map->job [jdbc-map]
  {:pre [jdbc-map]}
  (-> (std/map-kv #(keyword "job" (name %)) identity jdbc-map)
      (update :job/status (partial get status))
      (update :job/type keyword)
      (util/when-update :job/args std/from-edn)
      (util/when-update :job/failure std/from-edn)))

(defn query-job
  "Return a Job map for the given job-id; throws if not found"
  [db-spec job-id]
  (->> {:select [:*] :from [:overseer_jobs] :where [:= :id job-id]}
       sql/format
       (j/query db-spec)
       first
       (std/flip select-keys [:id :type :status :args :failure :heartbeat])
       jdbc-map->job))

(defn query-lock-version [db-spec job-id]
  (->> {:select [:lock_version] :from [:overseer_jobs] :where [:= :id job-id]}
       sql/format
       (j/query db-spec)
       first
       :lock_version))

(defn update-job
  "Attempt to update attributes of a single job using optimistic locking;
  Returns job-id if record updated, else nil if job is stale

  where-map - map of {keyword-column-name value} to scope update. id/lock_version
              are automatically added to the final where clause
  set-map - map of {keyword-column-name value} of new attributes to set"
  [db-spec job-id where-map set-map]
  (let [lock-version (query-lock-version db-spec job-id)

        where-map'
        (merge {:id job-id :lock_version lock-version} where-map)

        set-map'
        (merge {:lock_version (inc lock-version) :updated_at (clj-time/now)} set-map)

        num-rows-updated
        (->> {:update :overseer_jobs
              :set set-map'
              :where (into [:and] (for [[k v] where-map'] [:= k v]))}
             sql/format
             (j/db-do-prepared db-spec)
             first)]
    ; If no rows updated - stale object, return nil
    (when (= 1 num-rows-updated)
      job-id)))

(defn- direct-dependents
  "Find the set of all Job IDs that directly depend on any of given `job-ids`"
  [db-spec job-ids]
  (->> {:select [:job_id]
        :modifiers [:distinct]
        :from [:overseer_dependencies]
        :where [:in :dep_id (seq job-ids)]}
       sql/format
       (j/query db-spec)
       (map :job_id)
       set))

(defn dependents [db-spec job-id]
  "Returns the set of all Job IDs that depend upon given `job-id`, both
  directly and transitively"
  (loop [all-dependents #{}
         visited #{}
         to-visit #{job-id}]
    (if (empty? to-visit)
      (set all-dependents)
      (let [dependents (direct-dependents db-spec to-visit)
            all-dependents' (set/union all-dependents dependents)
            visited' (set/union visited to-visit)
            to-visit' (set/difference dependents visited')]
        (recur all-dependents' visited' to-visit')))))

(defn job-dep-jdbc-maps
  "Return a seq of dependency rows to insert for a Graph<Job>"
  [graph]
  (let [edge->dep
        (fn [[src dest]]
          {:job_id (:job/id src)
           :dep_id (:job/id dest)})]
    (->> graph
         loom.graph/edges
         (map edge->dep))))

(def default-table-opts
  "Backend-specific system configuration options"
  {:mysql
   {:table-spec "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=DYNAMIC"}})

(defn install' [adapter db-spec]
  (j/db-do-commands
    db-spec
    [(j/create-table-ddl
       :overseer_jobs
       [[:id "VARCHAR(64) PRIMARY KEY"]
        [:type "VARCHAR(255)"]
        [:args "VARCHAR(2048)"]
        [:status :tinyint]
        [:failure "VARCHAR(2048)"]
        [:heartbeat :int]
        [:lock_version "INT NOT NULL DEFAULT '0'"]
        [:created_at :datetime]
        [:updated_at :datetime]]
       (get default-table-opts adapter {}))
     (j/create-table-ddl
       :overseer_dependencies
       [[:job_id "VARCHAR(64)"]
        [:dep_id "VARCHAR(64)"]]
       (get default-table-opts adapter {}))
     "CREATE INDEX index_overseer_jobs_on_status ON overseer_jobs (status)"
     "CREATE INDEX index_overseer_dependencies_on_job_id ON overseer_dependencies (job_id)"
     "CREATE INDEX index_overseer_dependencies_on_dep_id ON overseer_dependencies (dep_id)"])
  :ok)

(defn dup-primary-key-ex? [adapter ex]
  (let [msg (.getMessage ex)]
    (case adapter
      :mysql (instance? MySQLIntegrityConstraintViolationException ex)
      :h2 (re-find #"^Unique index or primary key violation" msg))))

(defrecord JdbcStore [adapter db-spec]
  core/Store
  (install [this]
    (install' adapter db-spec))

  (transact-graph [this graph]
    (assert (core/valid-graph? graph))
    (let [now (clj-time/now)
          job-rows (->> graph
                        loom.graph/nodes
                        (map job->jdbc-map)
                        (map #(assoc % :created_at now :updated_at now)))
          dep-rows (job-dep-jdbc-maps graph)]
      (try
        (j/with-db-transaction [t-con db-spec]
          (j/insert-multi! t-con :overseer_jobs job-rows)
          (j/insert-multi! t-con :overseer_dependencies dep-rows))
        graph
        (catch Exception ex
          (if (dup-primary-key-ex? adapter ex)
            graph
            (throw ex))))))

  (job-info [this job-id]
    (query-job db-spec job-id))

  (reserve-job [this job-id]
    (let [where-map {:status (:unstarted status-code)}
          set-map {:status (:started status-code)
                   :heartbeat (core/heartbeat)}]
      (when (update-job db-spec job-id where-map set-map)
        (query-job db-spec job-id))))

  (finish-job [this job-id]
    (let [where-map {:status (:started status-code)}
          set-map {:status (:finished status-code)}]
      (when-not (update-job db-spec job-id where-map set-map)
        (throw (ex-info "Job update failed" {:job/id job-id})))))

  (fail-job [this job-id failure]
    (let [where-map {:status (:started status-code)}
          set-map {:status (:failed status-code)
                   :failure (pr-str failure)}]
      (when-not (update-job db-spec job-id where-map set-map)
        (throw (ex-info "Job update failed" {:job/id job-id})))))

  (heartbeat-job [this job-id]
    (when-not (update-job db-spec job-id {} {:heartbeat (core/heartbeat)})
      (throw (ex-info "Job update failed" {:job/id job-id}))))

  (abort-job [this job-id]
    (let [job-ids (cons job-id (dependents db-spec job-id))]
      (->> {:update :overseer_jobs
            :set {:status (:aborted status-code)
                  :updated_at (clj-time/now)}
            :where [:in :id job-ids]}
           sql/format
           (j/db-do-prepared db-spec))))

  (reset-job [this job-id]
    (let [where-map {:status (:started status-code)}
          set-map {:status (:unstarted status-code)
                   :heartbeat (core/heartbeat)}]
      ; Okay to ignore CAS failure here
      (update-job db-spec job-id where-map set-map)))

  (jobs-ready [this]
    (->> {:select [:id]
          :from [:overseer_jobs]
          :where
          [:and
           [:= :status (:unstarted status-code)]
           [:not-in :id
            {:select [:job_id] :from [:overseer_dependencies]
             :join [:overseer_jobs [:= :overseer_jobs.id :overseer_dependencies.dep_id]]
             :where [:not= :overseer_jobs.status (:finished status-code)]}]]}
         sql/format
         (j/query db-spec)
         (map :id)))

  (jobs-dead [this thresh]
    (->> {:select [:id]
          :from [:overseer_jobs]
          :where [:and
                  [:= :status (:started status-code)]
                  [:< :heartbeat thresh]]}
         sql/format
         (j/query db-spec)
         (map :id))))

(defn store [{:keys [adapter db-spec] :as config}]
  {:pre [adapter db-spec]}
  (->JdbcStore adapter db-spec))
