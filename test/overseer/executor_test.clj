(ns overseer.executor-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (overseer
              [test-utils :as test-utils]
              [executor :as exc])))

(deftest test-reserve-job
 (timbre/with-log-level :report
   (let [conn (test-utils/connect)
         exception-handler (fn [_] :failed)
         job (test-utils/->transact-job conn)
         job-ent-id (:db/id job)]
     (is (= job
            (exc/reserve-job exception-handler conn job)))
     (is (= :started
            (:job/status (d/entity (d/db conn) job-ent-id)))))))

(deftest test-run-job-success
  (timbre/with-log-level :report
    (let [config {}
          conn (test-utils/connect)
          job-handlers {:foo (fn [job] :ok)}
          job (test-utils/->transact-job conn {:job/type :foo})
          job-ent-id (:db/id job)
          status-txns (exc/run-job config conn job-handlers job)
          {:keys [db-before db-after]} @(d/transact conn status-txns)]
      (is (= :unstarted (:job/status (d/entity db-before job-ent-id))))
      (is (= :finished (:job/status (d/entity db-after job-ent-id)))))))

(deftest test-run-job-failure
  (timbre/with-log-level :report
    (let [config {}
          conn (test-utils/connect)
          job-handlers {:bar (fn [sys job] (throw (Exception. "uh oh")))}
          job (test-utils/->transact-job conn {:job/type :bar})
          job-ent-id (:db/id job)
          status-txns (exc/run-job config conn job-handlers job)
          {:keys [db-before db-after]} @(d/transact conn status-txns)]
      (is (= :unstarted (:job/status (d/entity db-before job-ent-id))))
      (is (= :failed (:job/status (d/entity db-after job-ent-id)))))))

(deftest test-invoke-handler
  (timbre/with-log-level :report
    (let [job {:job/type :foo}
          handler-fn (fn [job] (reduce + 0 [1 2 3 4 5]))
          handler-map {:pre-process (fn [job]
                                      (assert (= (:job/type job) :foo)))
                       :process (fn [job]
                                  (assert (= (:job/type job) :foo))
                                  [1 2 3 4 5])
                       :post-process (fn [job res] (reduce + 0 res))}
          bad-handler "not-fn-or-map"]
      (is (= 15 (exc/invoke-handler handler-fn job)))
      (is (= 15 (exc/invoke-handler handler-map job)))
      (is (thrown? Exception (exc/invoke-handler bad-handler job))))))
