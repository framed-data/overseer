(ns ^:no-doc overseer.worker
  "A Worker is the main top-level unit of Overseer. Internally, it acts as a
   supervisor for several processes that coordinate to select ready
   jobs from the queue and execute them"
  (:require [taoensso.timbre :as timbre]
            [framed.std.core :as std :refer [future-loop]]
            (overseer
              [config :as config]
              [core :as core]
              [executor :as exc]
              [heartbeat :as heartbeat])
            [overseer.store.datomic :as datomic-store]))

(defn start!
  "Run a worker. Takes a config and handlers as a map of {job-type job-handler}."
  [config job-handlers]
  (timbre/info "Worker starting!")
  (let [store (datomic-store/store (config/datomic-uri config))
        ready-jobs (atom #{})
        current-job (atom nil)

        detector-fut
        (future-loop
          (reset! ready-jobs (core/ready-job-info store job-handlers))
          (Thread/sleep (config/detector-sleep-time config)))

        executor-fut
        (exc/start-executor config store job-handlers ready-jobs current-job)

        heartbeat-fut
        (when (config/heartbeat? config)
          (heartbeat/start-heartbeat config store current-job))

        heartbeat-monitor-fut
        (when (config/heartbeat? config)
          (heartbeat/start-monitor config store))]
    (std/map-from-keys
      detector-fut
      executor-fut
      heartbeat-fut
      heartbeat-monitor-fut)))
