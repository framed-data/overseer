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
              [heartbeat :as heartbeat])))

(defn ready-job-info
  "Return the set of job entities that are ready to execute,
  filtering to those defined in job-handlers (this allows different
  nodes to solely execute certain types of jobs, if desired)"
  [store job-handlers]
  (->> (core/jobs-ready store)
       (map (partial core/job-info store))
       (filter (comp job-handlers :job/type))
       set))

(defn start! [config store job-handlers]
  (timbre/info "Worker starting")
  (let [ready-jobs (atom #{})
        current-job (atom nil)

        detector-fut
        (future-loop
          (reset! ready-jobs (ready-job-info store job-handlers))
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
