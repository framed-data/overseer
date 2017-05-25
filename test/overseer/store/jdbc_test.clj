(ns overseer.store.jdbc-test
 (:require [clojure.test :refer :all]
           [clojure.java.jdbc :as j]
           [clojure.string :as string]
           [framed.std.core :as std]
           [honeysql.core :as sql]
           loom.graph
           (overseer
             [api :as api]
             [core :as core]
             [test-utils :as test-utils])
           [overseer.store.jdbc :as store]
           [overseer.store-test :as store-test]
           [overseer.heartbeat-test :as heartbeat-test]))

(deftest test-job->jdbc-map
  (let [job {:job/id "foo-id"
             :job/type "process"
             :job/status :unstarted
             :job/args {:customer_id 1}}]
    (is (= {:id "foo-id"
            :type "process"
            :status (:unstarted store/status-code)
            :args (pr-str {:customer_id 1})}
           (store/job->jdbc-map job)))))

(deftest test-jdbc-map->job
  (let [jdbc-map {:id "foo-id"
                  :type "process"
                  :status (:unstarted store/status-code)
                  :args (pr-str {:customer_id 1})}]
    (is (= {:job/id "foo-id"
            :job/type :process
            :job/status :unstarted
            :job/args {:customer_id 1}}
           (store/jdbc-map->job jdbc-map)))))

(deftest test-job-jdbc-map-roundtrip
  (let [job (test-utils/job)]
    (is (= job (store/jdbc-map->job (store/job->jdbc-map job))))))

(deftest test-update-job
  ; Test that optimistic concurrency is implemented correctly (via lock_version)
  ; Start two concurrent update attempts on the same row; expect only 1 to win
  ; and subsequently update the row / increment the lock
  (let [{:keys [db-spec] :as store} (test-utils/jdbc-store)
        {job-id :job/id :as job} (test-utils/job)

        conflicts (atom 0)

        attempt-update
        (fn []
          (when-not (store/update-job db-spec job-id {} {:status (:started store/status-code)})
            (swap! conflicts inc)))]
    (core/transact-graph store (api/simple-graph job))
    (run! deref [(future (attempt-update))
                 (future (attempt-update))])
    (is (= 1 @conflicts))
    (is (= 1 (store/query-lock-version db-spec job-id)))
    (is (= :started (:job/status (store/query-job db-spec job-id))))))

(deftest test-dependents
  (let [{:keys [db-spec] :as store} (test-utils/jdbc-store)

        j0 (test-utils/job {:job/id "j0"})
        j1 (test-utils/job {:job/id "j1"})
        j2 (test-utils/job {:job/id "j2"})

        graph
        (loom.graph/digraph
          {j0 []
           j1 [j0]
           j2 [j1]})]
    (core/transact-graph store graph)
    (is (= #{"j1" "j2"} (store/dependents db-spec "j0")))))

(deftest test-job-dep-jdbc-maps
  (let [j0 (test-utils/job {:job/id "j0"})
        j1 (test-utils/job {:job/id "j1"})
        j2 (test-utils/job {:job/id "j2"})
        j3 (test-utils/job {:job/id "j3"})

        graph
        (loom.graph/digraph
          {j0 []
           j1 [j0]
           j2 []
           j3 [j1 j2]})]
    (is (= #{{:job_id "j1" :dep_id "j0"}
             {:job_id "j3" :dep_id "j1"}
             {:job_id "j3" :dep_id "j2"}}
           (set (store/job-dep-jdbc-maps graph))))))

(deftest test-transact-graph
  (let [{:keys [db-spec] :as store} (test-utils/jdbc-store)
        args {:org/id 123}
        graph (core/job-graph
                {:start []
                 :process [:start]}
                args)

        jobs-by-type
        (->> graph
             loom.graph/nodes
             (group-by :job/type)
             (std/map-kv first))

        _ (core/transact-graph store graph)]
    (testing "job rows"
      (let [jobs (j/query db-spec (sql/format {:select [:*] :from [:overseer_jobs]}))]
        (is (= 2 (count jobs)))
        (is (every? (partial = (:unstarted store/status-code)) (map :status jobs)))
        (is (every? (partial = (pr-str args)) (map :args jobs)))
        (is (every? (partial = 0) (map :lock_version jobs)))
        (is (every? identity (map :created_at jobs)))
        (is (every? identity (map :updated_at jobs)))))
    (testing "dependency rows"
      (let [deps (j/query db-spec (sql/format {:select [:*] :from [:overseer_dependencies]}))
            start-id (:job/id (:start jobs-by-type))
            process-id (:job/id (:process jobs-by-type))]
        (is (= 1 (count deps)))
        (is (= process-id (:job_id (first deps))))
        (is (= start-id (:dep_id (first deps))))))
    (testing "idempotent insert"
      (let [count-jobs
            (fn []
              (->> {:select [[:%count.* "job_count"]] :from [:overseer_jobs]}
                   sql/format
                   (j/query db-spec)
                   first
                   :job_count))]
        (is (= graph (core/transact-graph store graph))
            "It returns graph after inserting into the store")
        (is (= 2 (count-jobs)))
        (is (= graph (core/transact-graph store graph))
            "It returns graph if job-ids already exist in the store")
        (is (= 2 (count-jobs)) "It is idempotent on job-ids")))))

(def ^:private sql-type-timestamp 93) ; From sql.h; corresponds to datetime

(deftest test-install
  (let [{:keys [db-spec] :as store} (test-utils/jdbc-store)]
    (testing "tables"
      (let [tables-by-name
            (->> {:select [:*] :from [:information_schema.tables]}
                 sql/format
                 (j/query db-spec)
                 (group-by (comp string/lower-case :table_name))
                 (std/map-kv first))]
        (is (contains? tables-by-name "overseer_jobs"))
        (is (contains? tables-by-name "overseer_dependencies"))) )
    (testing "columns"
      (let [cols-by-name
            (fn [table-name]
              (->> {:select [:*] :from [:information_schema.columns] :where [:= :table_name table-name]}
                   sql/format
                   (j/query db-spec)
                   (group-by (comp string/lower-case :column_name))
                   (std/map-kv first)))

            job-cols-by-name (cols-by-name "OVERSEER_JOBS")
            dep-cols-by-name (cols-by-name "OVERSEER_DEPENDENCIES")]
        (are [ty col] (= ty (get-in job-cols-by-name [col :type_name]))
          "VARCHAR" "id"
          "VARCHAR" "type"
          "VARCHAR" "args"
          "TINYINT" "status"
          "VARCHAR" "failure"
          "INTEGER" "heartbeat"
          "INTEGER" "lock_version")
        (is (= sql-type-timestamp (get-in job-cols-by-name ["created_at" :data_type])))
        (is (= sql-type-timestamp (get-in job-cols-by-name ["updated_at" :data_type])))
        (are [ty col] (= ty (get-in dep-cols-by-name [col :type_name]))
          "VARCHAR" "job_id"
          "VARCHAR" "dep_id")))))

(deftest test-protocol
  (store-test/test-protocol test-utils/jdbc-store))

(deftest test-heartbeat
  (heartbeat-test/test-heartbeats test-utils/jdbc-store))
