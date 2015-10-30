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
              [status :as status])))

(def detector-fut
  "A Future for the ready job detector process, responsible
   for periodically updating a local cache used when starting
   new jobs"
  (atom nil))

(def detector-sleep-time
  "Pause time between ready job detector runs (ms)"
  2000)

(def executor-fut
  "A Future for the main job executor process"
  (atom nil))

(def supervisor-fut
  "A Future for a supervisor process that continually checks the
   completion status of the currently-running job, and terminates
   the process if it has since been finished (to be restarted by
   an external supervisor)"
  (atom nil))

(def supervisor-sleep-time
  "Pause time between supervisor job completion checks (ms)"
  10000)

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
        current-job (atom nil)]
    (reset! detector-fut
      (future-loop
        (reset! ready-jobs (ready-job-entities (d/db conn) job-handlers))
        (Thread/sleep detector-sleep-time)))

    (reset! executor-fut (exc/->executor config conn job-handlers ready-jobs current-job))

    (when (config/supervise? config)
      (reset! supervisor-fut
        (future-loop
          (when-let [{job-id :job/id} @current-job]
            (let [current-status (:job/status (d/entity (d/db conn) [:job/id job-id]))]
              (when (and (= :finished current-status)
                         (= job-id (:job/id @current-job))) ; Short jobs could have finished already!
                (timbre/info (format "Supervisor detected completion of %s; terminate executor" job-id))
                (System/exit 1))))
          (Thread/sleep supervisor-sleep-time))))

    (std/map-from-keys detector-fut executor-fut supervisor-fut)))
