(ns ^:no-doc overseer.worker
  "A Worker is the main top-level unit of Overseer. Internally, it acts as a
   supervisor for several processes that coordinate to select ready
   jobs from the queue and execute them"
  (:require [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [framed.std.core :as std :refer [future-loop]]
            (overseer
              [config :as config]
              [core :as core]
              [executor :as exc]
              [heartbeat :as heartbeat]
              [status :as status])))

(def detector-sleep-time
  "Pause time between ready job detector runs (ms)"
  2000)

(defn ready-job-entities [db job-handlers]
  (->> (status/jobs-ready db)
       (map (partial core/->job-entity db))
       (filter (comp job-handlers :job/type))))

(defn start!
  "Run a worker. Takes a config and handlers as a map of {job-type job-handler}."
  [config job-handlers]
  (timbre/info "Worker starting!")
  (let [conn (d/connect (config/datomic-uri config))
        ready-jobs (atom [])
        current-job (atom nil)

        detector-fut
        (future-loop
          (reset! ready-jobs (ready-job-entities (d/db conn) job-handlers))
          (Thread/sleep detector-sleep-time))

        executor-fut
        (exc/start-executor config conn job-handlers ready-jobs current-job)

        heartbeat-fut
        (when (config/heartbeat? config)
          (heartbeat/start-heartbeat config conn current-job))

        heartbeat-monitor-fut
        (when (config/heartbeat? config)
          (heartbeat/start-monitor config conn))]
    (std/map-from-keys
      detector-fut
      executor-fut
      heartbeat-fut
      heartbeat-monitor-fut)))
