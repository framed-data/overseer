(ns overseer.worker
  (:require [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (raven-clj
               [core :as raven]
               [interfaces :as raven.interface])
            (overseer
              [core :as core]
              [status :as status])))

(def signal (atom nil))

(defn signal! [sig status]
  (reset! sig status))

(defn try-thunk
  "Returns the value of calling f, or of calling exception-handler
   with any exception thrown"
  [exception-handler f]
  (try (f)
    (catch Throwable ex
      (exception-handler ex))))

(defn ready-job-entities [db job-handlers]
  (->> (status/jobs-ready db)
       (map (partial core/->job-entity db))
       (filter (comp job-handlers :job/type))))

(defn reserve-job
  "Attempt to reserve a job and return it, or return nil on failure"
  [exception-handler conn job]
  (let [job-id (:job/id job)]
    (try-thunk exception-handler
      #(do (timbre/info "Reserving job" job-id)
           (core/reserve conn job-id)
           (timbre/info "Reserved job" job-id)
           job))))

(defn sentry-capture [dsn ex]
  (let [extra (or (ex-data ex) {})
        ex-map
        (-> {:message (.getMessage ex)
             :extra extra}
            (raven.interface/stacktrace ex))]
    (try (raven/capture dsn ex-map)
      (catch Exception ex'
        (timbre/error "Senry exception handler failed")
        (timbre/error ex')))))

(defn ->default-exception-handler
  "Construct an handler function that by default logs exceptions
   and optionally sends to Sentry if configured"
  [{:keys [config] :as system}]
  (fn [ex]
    (timbre/error ex)
    (if-let [dsn (get-in config [:sentry :dsn])]
      (sentry-capture dsn ex))
    nil))

(defn ->job-exception-handler
  "Exception handler for job thunks; invokes the default handler,
   then returns a status keyword. Attempts to parse special signal
   status out of ex, else defaults to :failed"
  [system ex]
  (fn [ex]
    (let [default-handler (->default-exception-handler system)]
      (default-handler ex)
      (or (get (ex-data ex) :overseer/status)
          :failed))))

(defn select-and-reserve
  "Attempt to select a ready job to run and reserve it, returning
   nil on failure"
  [{:keys [conn] :as system} jobs conn]
  (let [job (core/->job-entity (d/db conn) (rand-nth jobs))
        ex-handler (->default-exception-handler system)]
    (when (reserve-job ex-handler conn job)
      job)))

(defn run-job
  "Run a single job and transact an appropriate status update"
  [{:keys [conn] :as system} job-handlers job]
  (let [job-id (:job/id job)
        job-handler (get job-handlers (:job/type job))
        exit-status (try-thunk (->job-exception-handler system)
                               #(job-handler system job))
        txns (core/update-job-status-txns (d/db conn) job-id exit-status)]
    (timbre/info "Exited job" job-id "with status" exit-status)
    (when (= exit-status :aborted)
      (timbre/info "Found :aborted job; aborting all dependents of" job-id))
    (d/transact conn txns)))

(defn ->job-executor
  "Construct a function that will reserve a job off the queue and execute it,
   returning nil if the queue is empty"
  [{:keys [config conn] :as system} job-handlers]
  (fn []
    (let [jobs (status/jobs-ready (d/db conn))]
      (when-not (empty? jobs)
        (timbre/info (count jobs) "handleable job(s) found.")
        (if-let [job (select-and-reserve system jobs conn)]
          (run-job system job-handlers job))))))

(defn run
  "Run a worker, given:
   1. A thunk that will pop a job and handle if applicable;
      returning nil if queue is empty (executed repeatedly)
   2. A signalling atom to control the worker
   3. The amount of time to sleep in between checking for jobs"
  [job-executor signal sleep-time]
  (loop []
    (if (= @signal :stop)
      (timbre/info ":stop received; stopping")
      (if (job-executor)
        (recur)
        (do (timbre/info "No handleable jobs found. Waiting.")
            (Thread/sleep sleep-time)
            (recur))))))

(defn start! [{:keys [config] :as system} job-handlers signal]
  (timbre/info "Worker starting!")
  (let [job-executor (->job-executor system job-handlers)
        sleep-time (:sleep-time config)]
    (run job-executor signal sleep-time)))

(defn stop! [signal]
  (signal! signal :stop))
