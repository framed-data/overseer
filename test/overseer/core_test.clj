(ns overseer.core-test
 (:require [clojure.test :refer :all]
           [clj-time.core :as clj-time]
           [datomic.api :as d]
           [framed.std.time :as std.time]
           [loom.graph :as graph]
           (overseer
             [core :as core]
             [status :as status]
             [test-utils :as test-utils])))

(deftest test-valid-job?
  (is (core/valid-job?
        {:job/id "123"
         :job/type :intake
         :job/status :started}))
  (is (core/valid-job?
        {:job/id "123"
         :job/type :intake
         :job/status :unstarted
         :job/heartbeat 12345}))
  (is (not (core/valid-job?
        {; No :job/id
         :job/type :intake
         :job/status :unstarted}))))

(deftest test-valid-graph?
  (testing "when graph is well-formed"
    (let [->job (fn [job-type]
                  {:job/id (str (java.util.UUID/randomUUID))
                   :job/type job-type
                   :job/status :unstarted})
          start-job (->job :start)
          result1-job (->job :result1)
          result2-job (->job :result2)
          finish-job (->job :finish)

          graph
          (loom.graph/digraph
            {start-job []
             result1-job [start-job]
             result2-job [start-job]
             finish-job [result1-job result2-job]})]
      (is (core/valid-graph? graph))))
  (testing "when graph does not satisfy Digraph interface"
    (let [j0 (test-utils/job {:job/type :start})
          j1 (test-utils/job {:job/type :step1})
          j2 (test-utils/job {:job/type :step2})

          ; Structurally this graph is well formed, but it does not satisfy
          ; `loom.graph/Digraph` (missing `loom.graph/digraph` call)
          map-graph
          {j0 []
           j1 [j0]
           j2 [j1]}]
      (is (not (core/valid-graph? map-graph)))))
  (testing "when graph contains invalid nodes"
    (let [j0 {:job/type :start} ; No :job/id or other required fields
          graph (loom.graph/digraph {j0 []})]
      (is (not (core/valid-graph? graph))))))

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
  (let [conn (test-utils/connect)
        graph [{:db/id (d/tempid :db.part/user -1001)
                :job/id "-1001"}
               {:db/id (d/tempid :db.part/user -1002)
                :job/id "-1002"
                :job/dep (d/tempid :db.part/user -1001)}
               {:db/id (d/tempid :db.part/user -1003)
                :job/dep (d/tempid :db.part/user -1001)
                :job/id "-1003"}
               {:db/id (d/tempid :db.part/user -1004)
                :job/dep (d/tempid :db.part/user -1002)
                :job/id "-1004"}
               {:db/id (d/tempid :db.part/user -1005)
                :job/id "-1005"}]
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

(deftest test-status-txn
  (testing "with no failure"
    (let [job-id "12345"
          status :finished
          job {:overseer/status status}
          output-txn (core/status-txn job job-id)]
      (is (= [:job/id job-id] (:db/id output-txn)))
      (is (= status (:job/status output-txn)))
      (is (= false (contains? output-txn :job/failure)))))
  (testing "with failure"
    (let [job-id "12345"
          status :failed
          failure {:foo :bar}
          job {:overseer/status status
               :overseer/failure failure}
          output-txn (core/status-txn job job-id)]
      (is (= [:job/id job-id] (:db/id output-txn)))
      (is (= status (:job/status output-txn)))
      (is (= (pr-str failure) (:job/failure output-txn))))))
