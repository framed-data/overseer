(ns overseer.heartbeat-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as clj-time]
            [framed.std.time :as std.time]
            [taoensso.timbre :as timbre]
            [overseer.api :as api]
            [overseer.core :as core]
            [overseer.heartbeat :as heartbeat]
            [overseer.test-utils :as test-utils]))

(defn test-start-heartbeat [store]
  (timbre/with-log-level :report
    (let [config {:heartbeat {:monitor-shutdown false}}
          old-heartbeat
          (-> (clj-time/now)
              (clj-time/minus (clj-time/days 30))
              std.time/datetime->unix)
          {job-id :job/id :as job} (test-utils/job {:job/heartbeat old-heartbeat})
          _ (core/transact-graph store (api/simple-graph job))
          current-job (atom (core/job-info store job-id))

          heartbeat-fut (heartbeat/start-heartbeat config store current-job)]
      (try
        (Thread/sleep 500)
        (is (> (:job/heartbeat (core/job-info store job-id))
               old-heartbeat)
            "It transacts heartbeats to the current job")
        (finally
          (test-utils/silent-cancel heartbeat-fut))))))

(defn test-start-monitor [store]
  (timbre/with-log-level :report
    (let [config {:heartbeat {:monitor-shutdown false}}
          job (test-utils/job
                {:job/heartbeat
                 (-> (clj-time/now)
                     (clj-time/minus (clj-time/days 30))
                     std.time/datetime->unix)})
          _ (core/transact-graph store (api/simple-graph job))

          monitor-fut (heartbeat/start-monitor config store)]
      (try
        (Thread/sleep 500)
        (is (= :unstarted
               (:job/status (core/job-info store (:job/id job))))
            "It resets jobs that have failed heartbeats")
        (finally
          (test-utils/silent-cancel monitor-fut))))))

;;

(defn test-heartbeats
  "Run a test suite exercises heartbeat functionality, given a nullary
  function to produce fresh Store instances"
  [store-factory]
  (test-start-heartbeat (store-factory))
  (test-start-monitor (store-factory)))
