(ns overseer.executor-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :as timbre]
            (overseer
              [core :as overseer]
              [test-utils :as test-utils]
              [executor :as exc])))

(deftest test-run-job-success
  (timbre/with-log-level :report
    (let [config {}
          store (test-utils/store)
          job-handlers {:foo (fn [job] :ok)}

          {job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (overseer/transact-graph store (overseer/simple-graph job))
      (is (= :unstarted (:job/status (overseer/job-info store job-id))))
      (overseer/reserve-job store job-id)
      (exc/run-job config store job-handlers job)
      (is (= :finished (:job/status (overseer/job-info store job-id)))))))

(deftest test-run-job-failure
  (timbre/with-log-level :report
    (let [config {}
          store (test-utils/store)
          job-handlers {:bar (fn [sys job] (throw (Exception. "uh oh")))}

          {job-id :job/id :as job} (test-utils/job {:job/type :bar})]
      (overseer/transact-graph store (overseer/simple-graph job))
      (is (= :unstarted (:job/status (overseer/job-info store job-id))))
      (overseer/reserve-job store job-id)
      (exc/run-job config store job-handlers job)
      (is (= :failed (:job/status (overseer/job-info store job-id)))))))

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
