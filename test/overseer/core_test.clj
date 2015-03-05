(ns overseer.core-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [core :as core]
             [status :as status]
             [test-utils :as test-utils])))

(use-fixtures :each test-utils/setup-db-fixtures)

(deftest test-missing-dependencies
  (let [g1 {:foo []
            :bar [:foo]}
        g2 {:baz [:quux]}] ; Invalid, quux not specified
    (is (= [] (core/missing-dependencies g1)))
    (is (= [:quux] (core/missing-dependencies g2)))))

(deftest test-missing-handlers
  (let [handlers {:foo (fn [_] nil)
                  :bar (fn [_] nil)}
        g1 {:foo []
            :bar [:foo]}

        g2 {:foo []
            :quux [:foo]}]
    (is (= [] (core/missing-handlers handlers g1)))
    (is (= [:quux] (core/missing-handlers handlers g2)))))

(deftest test-jobs-ready
  (let [conn (test-utils/connect)
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
        jobs-ready (status/jobs-ready db-after)
        ready? (fn [ent-id]
                 (contains? jobs-ready (:job/id (d/entity db-after ent-id))))]
    (is (ready? (resolve-tempid -1000))
        "It finds :unstarted jobs with no dependencies")
    (is (ready? (resolve-tempid -1001))
        "It finds :started jobs with no dependencies")
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
  (let [conn (test-utils/connect)
        graph [{:db/id (d/tempid :db.part/user -1001)
                :job/id "12345"}
               {:db/id (d/tempid :db.part/user -1002)
                :job/dep (d/tempid :db.part/user -1001)}
               {:db/id (d/tempid :db.part/user -1003)
                :job/dep (d/tempid :db.part/user -1001)}
               {:db/id (d/tempid :db.part/user -1004)
                :job/dep (d/tempid :db.part/user -1002)}
               {:db/id (d/tempid :db.part/user -1005)
                :job/id "67890"}]
        {:keys [tempids db-after] :as txn} @(d/transact conn graph)
        resolve-tempid (fn [tempid]
                         (->> (d/tempid :db.part/user tempid)
                              (d/resolve-tempid db-after tempids)) )
        entity-ids (map resolve-tempid [-1002 -1003 -1004])
        ->job-id #(:job/id (d/entity db-after %))]
    (is (= (set (map ->job-id entity-ids))
           (core/transitive-dependents db-after (->job-id (resolve-tempid -1001)))))
    (is (= #{}
           (core/transitive-dependents db-after (->job-id (resolve-tempid -1005)))))))

(deftest test-job-dep-edges
  (let [graph
        {:start []
         :result1 [:start]
         :result2 [:start]
         :publish [:result1 :result2]}
        jobs-by-type (core/job-assertions-by-type (keys graph) {})
        ->job-tempid (fn [job-type] (:db/id (get jobs-by-type job-type)))
        edge-assertions (core/job-dep-edges graph jobs-by-type)]
      (is (= (->job-tempid :start)
             (:job/dep (nth edge-assertions 0)))
          ":result1 depends on :start")

      (is (= (->job-tempid :start)
             (:job/dep (nth edge-assertions 1)))
          ":result2 depends on :start")

      (is (= (->job-tempid :result1)
             (:job/dep (nth edge-assertions 2)))
          ":publish depends on :result1")

      (is (= (->job-tempid :result2)
             (:job/dep (nth edge-assertions 3)))
          ":publish depends on :result2")))
