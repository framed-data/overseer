(ns ^:no-doc overseer.heartbeat
  "Processes to send heartbeats for running jobs, and monitor
   other jobs for failures

   Note that the system does *not* support running in a degraded state;
   if any component here experiences an error the entire system will shutdown
   (presumably to be restarted by an external process supervisor)"
  (:require [clojure.string :as string]
            [clj-time.core :as tcore]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (framed.std
              [core :as std :refer [future-loop]]
              [time :as std.time])
            (overseer
              [config :as config]
              [core :as core])))

(defn start-heartbeat
  "Start a process that will continually persist heartbeats via the DB"
  [config conn current-job]
  (future-loop
    (try
      (when-let [{job-id :job/id} @current-job]
        (let [heartbeat (std.time/datetime->unix (tcore/now))]
          @(d/transact conn [{:db/id [:job/id job-id]
                              :job/heartbeat heartbeat}])))
      (Thread/sleep (config/heartbeat-sleep-time config))
      (catch Exception ex
        (timbre/error ex)
        (System/exit 1))))) ; Conservative, avoid running in degraded state

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

(defn dead-jobs
  "Return a seq of started jobs whose last heartbeat occurred before some
   liveness threshold (Unix timestamp)"
  [db thresh]
  (->> (d/q '[:find [?jid ...]
              :in $ ?thresh
              :where [?e :job/status :started]
                     [?e :job/heartbeat ?h]
                     [(< ?h ?thresh)]
                     [?e :job/id ?jid]]
            db
            thresh)
       (mapv (partial core/->job-entity db))))

(defn- sleep-stagger
  "Generate a random stagger interval in ms so that monitors started
   around the same time are not constantly clashing"
  []
  (* 1000 (std/rand-int-between 1 10)))

(defn- reset-job-txns
  "Given a job entity that has failed heartbeat, return txns to
   reset its status to unstarted and retract its heartbeat (to prevent
   erroneous reset by another monitor)"
  [{ent-id :db/id heartbeat :job/heartbeat :as job}]
  [[:db/retract ent-id :job/heartbeat heartbeat]
   [:db.fn/cas ent-id :job/status :started :unstarted]])

(defn cas-failed?
  "Return whether an exception is specifically a Datomic check-and-set failure"
  [ex]
  (= :db.error/cas-failed
     (->> ex
          Throwable->map
          :data
          :db/error)))

(defn start-monitor
  "Start a process that will continually check for jobs failing
   heartbeats and reset them"
  [config conn]
  (future-loop
    (try
      (let [db (d/db conn)
            thresh (liveness-threshold config (tcore/now))
            jobs (dead-jobs db thresh)
            dead-job-txns (mapcat reset-job-txns jobs)]
        (when (seq dead-job-txns)
          (timbre/warn (format "Found %s jobs with failed heartbeats" (count jobs)))
          (timbre/warn (str "Resetting: " (string/join ", " (map :job/id jobs))))
          @(d/transact conn dead-job-txns))
        (Thread/sleep (+ (config/heartbeat-sleep-time config) (sleep-stagger))))
      (catch java.util.concurrent.ExecutionException ex
        (if (cas-failed? ex)
          (timbre/info "Heartbeat monitor CAS failed; ignoring.")
          (throw ex)))
      (catch Exception ex
        (timbre/error ex)
        (System/exit 1))))) ; Conservative, avoid running in degraded state
