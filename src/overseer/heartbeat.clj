(ns ^:no-doc overseer.heartbeat
  "Processes to send heartbeats for running jobs, and monitor
  other jobs for failures

  Note that by default the system does not support running in a degraded state;
  if the heartbeat monitor experiences an error the entire system will shutdown
  (presumably to be restarted by an external process supervisor)
  See `overseer.config` to configure this behavior"
  (:require [clojure.string :as string]
            [clj-time.core :as tcore]
            [taoensso.timbre :as timbre]
            (framed.std
              [core :as std :refer [future-loop]]
              [time :as std.time])
            (overseer
              [config :as config]
              [core :as core])))

(defn start-heartbeat
  "Start a process that will continually persist heartbeats via the DB
  `current-job` is an atom holding the currently running Job (at least its :job/id)"
  [config store current-job]
  (future-loop
    (try
      (when-let [{job-id :job/id} @current-job]
        (core/heartbeat-job store job-id))
      (Thread/sleep (config/heartbeat-sleep-time config))
      (catch Exception ex
        (timbre/error ex)
        (when (config/monitor-shutdown? config)
          (System/exit 1))))))

;;

(defn- liveness-threshold
  "Return the Unix timestamp relative to `now` such that any job with a heartbeat
   prior to threshold is considered dead"
  [config now]
  (->> (tcore/minus
         now
         (tcore/millis (* (config/failed-heartbeat-tolerance config)
                          (config/heartbeat-sleep-time config))))
       std.time/datetime->unix))

(defn- sleep-stagger
  "Generate a random stagger interval in ms so that monitors started
   around the same time are not constantly clashing"
  []
  (* 1000 (std/rand-int-between 1 10)))

(defn start-monitor
  "Start a process that will continually check for jobs failing
   heartbeats and reset them"
  [config store]
  (future-loop
    (try
      (let [thresh (liveness-threshold config (tcore/now))
            job-ids (core/jobs-dead store thresh)]
        (when (seq job-ids)
          (timbre/warn (format "Found %s jobs with failed heartbeats" (count job-ids)))
          (timbre/warn (str "Resetting: " (string/join ", " job-ids)))
          (doseq [id job-ids]
            (core/reset-job store id)))
        (Thread/sleep (+ (config/heartbeat-sleep-time config) (sleep-stagger))))
      (catch Exception ex
        (timbre/error ex)
        (when (config/monitor-shutdown? config)
          (System/exit 1))))))
