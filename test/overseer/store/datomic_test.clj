(ns overseer.store.datomic-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [core :as core]
             [test-utils :as test-utils])
           [overseer.store.datomic :as store]
           [taoensso.timbre :as timbre]
           [clj-time.core :as tcore]
           [framed.std.time :as std.time]
           [overseer.store-test :as store-test]
           loom.graph))

(defn test-datomic-conn []
  (d/connect (test-utils/bootstrap-datomic-uri)))

(defn test-store []
  (store/store (test-utils/bootstrap-datomic-uri)))

(deftest test-jobs-ready
  (let [conn (test-datomic-conn)
        db (d/db conn)
        ->squuid #(str (d/squuid))
        jobs-txn
        [{:db/id (d/tempid :db.part/user -1000)
          :job/id (->squuid)
          :job/status :unstarted}
         {:db/id (d/tempid :db.part/user -1001)
          :job/id (->squuid)
          :job/status :started}
         {:db/id (d/tempid :db.part/user -1002)
          :job/id (->squuid)
          :job/status :unstarted
          :job/dep [(d/tempid :db.part/user -1000)]}
         {:db/id (d/tempid :db.part/user -1003)
          :job/id (->squuid)
          :job/status :finished}
         {:db/id (d/tempid :db.part/user -1004)
          :job/id (->squuid)
          :job/status :unstarted
          :job/dep [(d/tempid :db.part/user -1003)]}
         {:db/id (d/tempid :db.part/user -1005)
          :job/id (->squuid)
          :job/status :unstarted
          :job/dep [(d/tempid :db.part/user -1000)
                    (d/tempid :db.part/user -1003)]}
         {:db/id (d/tempid :db.part/user -1006)
          :job/id (->squuid)
          :job/status :aborted}]
        {:keys [tempids db-after] :as txn} @(d/transact conn jobs-txn)
        resolve-tempid (fn [tempid]
                         (->> (d/tempid :db.part/user tempid)
                              (d/resolve-tempid db-after tempids)) )
        jobs-ready (store/jobs-ready' db-after)
        ready? (fn [ent-id]
                 (contains? jobs-ready (:job/id (d/entity db-after ent-id))))]
    (is (ready? (resolve-tempid -1000))
        "It finds :unstarted jobs with no dependencies")
    (is (not (ready? (resolve-tempid -1001)))
        "It excludes :started jobs")
    (is (not (ready? (resolve-tempid -1002)))
        "excludes jobs with unfinished dependencies")
    (is (not (ready? (resolve-tempid -1003)))
        "It excludes jobs that are already :finished")
    (is (ready? (resolve-tempid -1004))
        "It finds jobs where all dependencies are satisfied.")
    (is (not (ready? (resolve-tempid -1005)))
        "It excludes jobs where only some dependencies are satisfied.")
    (is (not (ready? (resolve-tempid -1006)))
        "It excludes jobs that are :aborted")))

(deftest test-transitive-dependents
  (let [conn (test-datomic-conn)
        graph [{:db/id (d/tempid :db.part/user -1001)
                :job/id "-1001"}
               {:db/id (d/tempid :db.part/user -1002)
                :job/id "-1002"
                :job/dep (d/tempid :db.part/user -1001)}
               {:db/id (d/tempid :db.part/user -1003)
                :job/id "-1003"
                :job/dep (d/tempid :db.part/user -1001)}
               {:db/id (d/tempid :db.part/user -1004)
                :job/id "-1004"
                :job/dep (d/tempid :db.part/user -1002)}
               {:db/id (d/tempid :db.part/user -1005)
                :job/id "-1005"}]
        {:keys [tempids db-after] :as txn} @(d/transact conn graph)]
    (is (= #{"-1002" "-1003" "-1004"}
           (store/transitive-dependents db-after "-1001"))
        "It returns all nodes that depend upon the given node, even indirectly.")
    (is (= #{} (store/transitive-dependents db-after "-1004"))
        "It doesn't return nodes that are only leaves")
    (is (= #{} (store/transitive-dependents db-after "-1005"))
        "It doesn't return disconnected nodes")))

(deftest test-protcol
  (store-test/test-protocol (test-store)))

(deftest test-transact-graph
  (let [j0 (test-utils/job {:job/type :start})
        j1 (test-utils/job {:job/type :step1})
        j2 (test-utils/job {:job/type :step2})

        ; Structurally this graph is well formed, but it does not satisfy
        ; `loom.graph/Digraph` (missing `loom.graph/digraph` call)
        invalid-graph
        {j0 []
         j1 [j0]
         j2 [j1]}]
    (is (thrown? AssertionError (core/transact-graph (test-store) invalid-graph)))))
