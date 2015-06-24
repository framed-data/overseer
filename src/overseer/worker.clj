(ns overseer.worker
  (:require [clj-json.core :as json]
            [clojure.edn :as edn]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (overseer
              [core :as core]
              [lottery :as lottery]
              [status :as status]
              [errors :as errors])))

(def default-sleep-time 10000) ; ms

(defn ready-job-entities [db job-handlers]
  (->> (status/jobs-ready db)
       (map (partial core/->job-entity db))
       (filter (comp job-handlers :job/type))))

(defn reserve-job
  "Attempt to reserve a job and return it"
  [exception-handler conn job]
  {:pre [job]}
  (let [{:keys [job/id job/type]} job]
    (errors/try-thunk exception-handler
      #(do (timbre/info (format "Reserving job %s (%s)" id type))
           (core/reserve conn id)
           (timbre/info "Reserved job" id)
           job))))

(defn select-and-reserve
  "Attempt to select a ready job to run and reserve it, returning
   nil on failure"
  [conn jobs]
  {:pre [(not (empty? jobs))]}
  (let [job (lottery/run-lottery jobs)]
    (when (reserve-job errors/reserve-exception-handler conn job)
      job)))

(defn invoke-handler
  "Invoke a job handler, which can either be an ordinary function
   expecting a job argument, or a map of the following structure:

     ; Optional, runs prior to main processing function and can be
     ; used to set up prerequisite state, for example
     :pre-process (fn [job] ...)

     ; Required, the main processing function.
     :process (fn [job args] ...)

     ; Optional, for post-processing after main execution. Receives
     ; the job and return value of the :process function as arguments.
     :post-process (fn [job res] ...)

     The default pre-processor passes the job through unmodified,
     and the default post-processor returns its result unmodified."
  [handler {:keys [job/args] :as job}]
  (cond
    (map? handler)
      (let [{:keys [pre-process process post-process]
             :or {pre-process (fn [job] job)
                  post-process (fn [job res] res)}} handler]
        (assert process "Map handler must define :process function")
        (pre-process job)
        (->> (process job (edn/read-string args))
             (post-process job)))
    (fn? handler)
      (handler job)
    :else
      (throw (Exception. "Handlers must either be a function or a map"))))

(defn run-job
  "Run a single job and return the appropriate status update txns"
  [config conn job-handlers job]
  (let [{job-id :job/id
         job-type :job/type} job
        handler (get job-handlers job-type)
        _ (assert handler (str "Handler not specified: " job-type))

        {:keys [overseer/status] :as exit-status-map}
        (errors/try-thunk (errors/->job-exception-handler config job)
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
  [config conn job-handlers]
  (fn []
    (let [jobs (ready-job-entities (d/db conn) job-handlers)]
      (when-not (empty? jobs)
        (timbre/info (count jobs) "handleable job(s) found.")
        (if-let [job (select-and-reserve conn jobs)]
          (let [txns (run-job config conn job-handlers job)]
            @(d/transact conn txns)))))))

(defn run
  "Run a worker, given:
   1. A thunk that will pop a job and handle if applicable;
      returning nil if queue is empty (executed repeatedly)
   3. The amount of time to sleep in between checking for jobs"
  [job-executor sleep-time]
  {:pre [sleep-time]}
  (loop []
    (if (job-executor)
      (recur)
      (do (timbre/info "No handleable jobs found. Waiting.")
          (Thread/sleep sleep-time)
          (recur)))))

(defn start! [config job-handlers]
  (timbre/info "Worker starting!")
  (let [conn (d/connect (get-in config [:datomic :uri]))
        job-executor (->job-executor config conn job-handlers)
        sleep-time (get config :sleep-time default-sleep-time)]
    (run job-executor sleep-time)))
