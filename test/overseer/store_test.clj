(ns overseer.store-test
  "Generic tests for implementations of the Store protocol.

  In your store's test namespace, call these, passing
  them a store implementation.

  (deftest my-test
    (overseer.store-test/test-fn my-store)"
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [api :as api]
             [core :as core]
             [test-utils :as test-utils])
           [overseer.store.datomic :as store]
           [taoensso.timbre :as timbre]
           [clj-time.core :as tcore]
           [framed.std.time :as std.time]
           loom.graph))

(def ->job test-utils/job)

(defn- unix<-millis-ago [now millis-ago]
  (->> (tcore/minus now (tcore/millis millis-ago))
       std.time/datetime->unix))

(defn test-job-info [store]
  (testing "arg serialization roundtripping"
    (let [job (test-utils/job
                {:job/type :foo
                 :job/args {:email "foo@example.com"
                            :age 30}})

          _ (core/transact-graph store (api/simple-graph job))
          job-after (core/job-info store (:job/id job))]
      (doseq [[k v] job]
        (is (= v (get job-after k)))))))

(defn test-updating-jobs [store]
  (testing "reservation"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (api/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (is (nil? (core/reserve-job store job-id))
          "It returns nil if it can't reserve an already started job")))

  (testing "heartbeating"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})
          _ (do
              (core/transact-graph store (api/simple-graph job))
              (core/reserve-job store job-id))
          job-before (core/job-info store job-id)
          _ (do
              (Thread/sleep 1000)
              (core/heartbeat-job store job-id))
          job-after (core/job-info store job-id)]
      (is (> (:job/heartbeat job-after) (:job/heartbeat job-before)))))

  (testing "resetting"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (api/simple-graph job))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (core/reset-job store job-id)
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (is (nil? (core/reset-job store job-id))
          "It returns nil if it can't reset an already unstarted job")))

  (testing "finishing"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (api/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (core/finish-job store job-id)
      (is (= :finished (:job/status (core/job-info store job-id))))))

  (testing "failing"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})
          failure {:thing :went-wrong}]
      (core/transact-graph store (api/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (core/fail-job store job-id failure)
      (is (= :failed (:job/status (core/job-info store job-id))))
      (is (= failure (:job/failure (core/job-info store job-id))))))

  (testing "aborting"
    (let [j0 (test-utils/job {:job/type :start})
          j1 (test-utils/job {:job/type :step1})
          j2 (test-utils/job {:job/type :step2})

          graph
          (loom.graph/digraph
            {j0 []
             j1 [j0]
             j2 [j1]})]
      (core/transact-graph store graph)
      (is (= :unstarted (:job/status (core/job-info store (:job/id j0)))))
      (is (= :unstarted (:job/status (core/job-info store (:job/id j1)))))
      (core/abort-job store (:job/id j0))
      (is (= :aborted (:job/status (core/job-info store (:job/id j0)))))
      (is (= :aborted (:job/status (core/job-info store (:job/id j1))))
          "It aborts all direct dependents")
      (is (= :aborted (:job/status (core/job-info store (:job/id j2))))
          "It aborts all transitive dependents"))))

(defn test-jobs-ready [store]
  (let [j0 (->job {:job/id "j0-id"})
        j1 (->job {:job/id "j1-id"})
        j2 (->job {:job/id "j2-id"})
        graph (loom.graph/digraph
                {j0 []
                 j1 [j0]
                 j2 [j1]})

        start-and-finish
        (fn [job-id]
          (core/reserve-job store job-id)
          (core/finish-job store job-id))

        _ (core/transact-graph store graph)
        jobs-ready (core/jobs-ready store)]
    (is (contains? jobs-ready "j0-id"))
    (is (not (contains? jobs-ready "j1-id")))
    (is (not (contains? jobs-ready "j2-id")))
    (start-and-finish "j0-id")
    (is (contains? (core/jobs-ready store) "j1-id"))
    (is (not (contains? jobs-ready "j2-id")))
    (start-and-finish "j1-id")
    (is (contains? (core/jobs-ready store) "j2-id"))))

(defn test-jobs-dead [store]
  (timbre/with-log-level :report
    (let [now (tcore/now)

          j1 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 1000)})
          j2 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 50000)}) ; Dead
          j3 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 500)})
          thresh (unix<-millis-ago now 3000)]
      (core/transact-graph store (api/simple-graph j1 j2 j3))
      (is (= [(:job/id j2)] (core/jobs-dead store thresh))))))

(defn test-protocol
  "Run a test suite exercising the Store protocol, given a nullary
  factory function to produce fresh Store instances"
  [store-factory]
  (test-job-info (store-factory))
  (test-updating-jobs (store-factory))
  (test-jobs-ready (store-factory))
  (test-jobs-dead (store-factory)))
