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
          system {:config {}}
          job {:job/id -1 :job/type :foo}
          ex-handler (w/->job-exception-handler system job)]
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
          system {:conn conn}
          job-handlers {:foo (fn [sys job] :ok)}
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
