(ns overseer.worker-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (overseer
              [test-utils :as test-utils]
              [worker :as w])))

(use-fixtures :each test-utils/setup-db-fixtures)

(deftest test-try-thunk
  (timbre/with-log-level :report
    (let [safe-f (fn [] :ok)
          unsafe-f (fn [] (throw (Exception. "uh oh")))
          exception-handler (fn [_] :failed)]
      (is (= :ok (w/try-thunk exception-handler safe-f)))
      (is (= :failed (w/try-thunk exception-handler unsafe-f))))))

(deftest test-job-exception-handler
  (timbre/with-log-level :report
    (let [ex (ex-info "uh oh" {:overseer/status :aborted})
          config {}
          job {:job/id -1 :job/type :foo}
          ex-handler (w/->job-exception-handler config job)]
      (is (= :aborted (ex-handler ex))))))

(deftest test-reserve-job
 (timbre/with-log-level :report
   (let [conn (test-utils/connect)
         exception-handler (fn [_] :failed)
         job (test-utils/->transact-job conn)
         job-ent-id (:db/id job)]
     (is (= job
            (w/reserve-job exception-handler conn job)))
     (is (= :started
            (:job/status (d/entity (d/db conn) job-ent-id)))))))

(deftest test-run-job-success
  (timbre/with-log-level :report
    (let [conn (test-utils/connect)
          system {:conn conn :config {}}
          job-handlers {:foo (fn [job] :ok)}
          job (test-utils/->transact-job conn {:job/type :foo})
          job-ent-id (:db/id job)
          status-txns (w/run-job system job-handlers job)
          {:keys [db-before db-after]} @(d/transact conn status-txns)]
      (is (= :unstarted (:job/status (d/entity db-before job-ent-id))))
      (is (= :finished (:job/status (d/entity db-after job-ent-id)))))))

(deftest test-run-job-failure
  (timbre/with-log-level :report
    (let [conn (test-utils/connect)
          system {:conn conn}
          job-handlers {:bar (fn [sys job] (throw (Exception. "uh oh")))}
          job (test-utils/->transact-job conn {:job/type :bar})
          job-ent-id (:db/id job)
          status-txns (w/run-job system job-handlers job)
          {:keys [db-before db-after]} @(d/transact conn status-txns)]
      (is (= :unstarted (:job/status (d/entity db-before job-ent-id))))
      (is (= :failed (:job/status (d/entity db-after job-ent-id)))))))

(deftest test-invoke-handler
  (timbre/with-log-level :report
    (let [job {:job/type :foo}
          handler-fn (fn [job] (reduce + 0 [1 2 3 4 5]))
          handler-map {:pre-process (fn [job]
                                      (assert (= (:job/type job) :foo))
                                      (assoc job :bar :quux))
                       :process (fn [job]
                                  (assert (= (:bar job) :quux))
                                  [1 2 3 4 5])
                       :post-process (fn [job res] (reduce + 0 res))}
          bad-handler "not-fn-or-map"]
      (is (= 15 (w/invoke-handler handler-fn job)))
      (is (= 15 (w/invoke-handler handler-map job)))
      (is (thrown? Exception (w/invoke-handler bad-handler job))))))
