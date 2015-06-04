(ns overseer.worker
  (:require [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (raven-clj
               [core :as raven]
               [interfaces :as raven.interface])
            (overseer
              [core :as core]
              [status :as status]
              [util :as util])))

(def default-sleep-time 10000) ; ms

(defn ready-job-entities [db job-handlers]
  (->> (status/jobs-ready db)
       (map (partial core/->job-entity db))
       (filter (comp job-handlers :job/type))))

(defn sentry-capture [dsn ex extra-info]
  (let [ex-map
        (-> {:message (.getMessage ex)
             :extra (merge (or extra-info {})
                           (or (util/sanitized-ex-data ex) {}))}
            (raven.interface/stacktrace ex))]
    (try (raven/capture dsn ex-map)
      (catch Exception ex'
        (timbre/error "Sentry exception handler failed")
        (timbre/error ex')))))

(defn ->default-exception-handler
  "Construct an handler function that by default logs exceptions
   and optionally sends to Sentry if configured"
  [config job]
  (fn [ex]
    (timbre/error ex)
    (when-not (= :aborted (:overseer/status (ex-data ex)))
      (if-let [dsn (get-in config [:sentry :dsn])]
        (sentry-capture dsn ex (select-keys job [:job/type :job/id]))))
    nil))

(defn failure-info
  "Construct a map of information about an exception, including
   user-supplied ex-data if present"
  [ex]
  (let [m {:overseer/status :failed
           :overseer/failure {:reason :system/exception
                              :exception (class ex)
                              :message (.getMessage ex)}}]
    (if-let [exc-data (ex-data ex)]
      (assoc-in m [:overseer/failure :data] exc-data)
      m)))

(defn ->job-exception-handler
  "Exception handler for job thunks; invokes the default handler,
   then returns a map of failure info, using user-provided ex-data if present.
   Attempts to parse special signal status out of ex, else defaults to :failed"
  [config job]
  (fn [ex]
    (let [default-handler (->default-exception-handler config job)]
      (default-handler ex))
      (failure-info ex)))

(defn reserve-job
  "Attempt to reserve a job and return it, or return nil on failure"
  [exception-handler conn job]
  {:pre [job]}
  (let [{:keys [job/id job/type]} job]
    (util/try-thunk exception-handler
      #(do (timbre/info (format "Reserving job %s (%s)" id type))
           (core/reserve conn id)
           (timbre/info "Reserved job" id)
           job))))

(defn select-and-reserve
  "Attempt to select a ready job to run and reserve it, returning
   nil on failure"
  [conn config jobs]
  {:pre [(not (empty? jobs))]}
  (let [job (rand-nth jobs)
        ex-handler (->default-exception-handler config job)]
    (when (reserve-job ex-handler conn job)
      job)))

(defn invoke-handler
  "Invoke a job handler, which can either be an ordinary function
   expecting a job argument, or a map of the following structure:

     ; Optional, runs prior to main processing function and can be
     ; used to transform input, for example.
     :pre-process (fn [job] ...)

     ; Required, the main processing function.
     :process (fn [job] ...)

     ; Optional, for post-processing after main execution. Receives
     ; the job and return value of the :process function as arguments.
     :post-process (fn [job res] ...)

     The default pre-processor passes the job through unmodified,
     and the default post-processor returns its result unmodified."
  [handler job]
  (cond
    (map? handler)
      (let [{:keys [pre-process process post-process]
             :or {pre-process (fn [job] job)
                  post-process (fn [job res] res)}} handler]
        (assert process "Expected handler map to define :process function")
        (->> (pre-process job)
             (process)
             (post-process job)))
    (fn? handler)
      (handler job)
    :else
      (throw (Exception. "Handlers must either be a function or a map"))))

(defn run-job
  "Run a single job and return the appropriate status update txns"
  [{:keys [config conn] :as system} job-handlers job]
  (let [{job-id :job/id
         job-type :job/type} job
        handler (get job-handlers job-type)
        _ (assert handler (str "Handler not specified: " job-type))

        {:keys [overseer/status] :as exit-status-map}
        (util/try-thunk (->job-exception-handler config job)
                        (fn []
                          (invoke-handler handler job)
                          {:overseer/status :finished}))
        txns (core/update-job-status-txns (d/db conn) job-id exit-status-map)]
    (timbre/info "Job" job-id "exited with status" status)
    (if (= status :aborted)
      (timbre/info "Found :aborted job; aborting all dependents of" job-id))
    txns))

(defn ->job-executor
  "Construct a function that will reserve a job off the queue and execute it,
   returning nil if the queue is empty"
  [{:keys [config conn] :as system} job-handlers]
  (fn []
    (let [jobs (ready-job-entities (d/db conn) job-handlers)]
      (when-not (empty? jobs)
        (timbre/info (count jobs) "handleable job(s) found.")
        (if-let [job (select-and-reserve conn config jobs)]
          (let [txns (run-job system job-handlers job)]
            @(d/transact conn txns)))))))

(defn run
  "Run a worker, given:
   1. A thunk that will pop a job and handle if applicable;
      returning nil if queue is empty (executed repeatedly)
   2. A signalling atom to control the worker
   3. The amount of time to sleep in between checking for jobs"
  [job-executor signal sleep-time]
  {:pre [sleep-time]}
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
        sleep-time (or (:sleep-time config) default-sleep-time)]
    (run job-executor signal sleep-time)))

(defn stop! [signal]
  (reset! signal :stop))
