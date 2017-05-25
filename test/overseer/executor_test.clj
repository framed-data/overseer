(ns overseer.executor-test
  (:require [clojure.test :refer :all]
            [taoensso.timbre :as timbre]
            (overseer
              [api :as api]
              [core :as core]
              [test-utils :as test-utils]
              [executor :as exc])))

(deftest test-run-job-success
  (timbre/with-log-level :report
    (let [config {}
          store (test-utils/datomic-store)
          job-handlers {:foo (fn [job] :ok)}

          {job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (api/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (exc/run-job config store job-handlers job)
      (is (= :finished (:job/status (core/job-info store job-id)))))))

(deftest test-run-job-failure
  (timbre/with-log-level :report
    (let [config {}
          store (test-utils/datomic-store)
          job-handlers {:bar (fn [job] (throw (Exception. "boom")))}

          {job-id :job/id :as job} (test-utils/job {:job/type :bar})]
      (core/transact-graph store (api/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (exc/run-job config store job-handlers job)
      (let [{:keys [job/status job/failure] :as job-after} (core/job-info store job-id)]
        (is (= :failed status))
        (is (= {:reason :system/exception
                :exception 'java.lang.Exception
                :message "boom"}
               failure))))))

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

(deftest test-start-executor
  (timbre/with-log-level :report
    (let [config {}
          store (test-utils/datomic-store)
          processed (atom 0)
          job-handlers {:process (fn [job] (swap! processed inc))}
          job (test-utils/job {:job/type :process})
          ready-jobs (atom #{job})
          current-job (atom nil)

          _ (core/transact-graph store (api/simple-graph job))

          exc-fut
          (exc/start-executor config store job-handlers ready-jobs current-job)]
      (try
        (Thread/sleep 500)
        (is (= 1 @processed)
            "It repeatedly reserves and executes ready jobs")
      (finally
        (test-utils/silent-cancel exc-fut))))))
