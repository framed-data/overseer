(ns ^:no-doc overseer.worker
  (:require [datomic.api :as d]
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
  [config db job-handlers job]
  (let [{job-id :job/id
         job-type :job/type} job
        handler (get job-handlers job-type)
        _ (assert handler (str "Handler not specified: " job-type))

        {:keys [overseer/status] :as exit-status-map}
        (errors/try-thunk (errors/->job-exception-handler config job)
                          (fn []
                            (invoke-handler handler job)
                            {:overseer/status :finished}))
        txns (core/update-job-status-txns db job-id exit-status-map)]
    (timbre/info "Job" job-id "exited with status" status)
    (if (= status :aborted)
      (timbre/info "Found :aborted job; aborting all dependents of" job-id))
    txns))

(defn start!
  "Run a worker.  Takes a config and a map {job-type job-handler}."
  [config job-handlers]
  (timbre/info "Worker starting!")
  (let [conn (d/connect (get-in config [:datomic :uri]))
        sleep-time (get config :sleep-time default-sleep-time)
        jobs (atom [])
        signal (atom true)]
    (future
      (while @signal
        (reset! jobs (ready-job-entities (d/db conn) job-handlers))
        (Thread/sleep 2000))
      (timbre/info "Stopping job detector."))
    (future
      (while @signal
        (if (empty? @jobs)
          (do (timbre/info "No handleable jobs found.  Wating.")
              (Thread/sleep sleep-time))
          (do (timbre/info "Found" (count @jobs) "handleable jobs.")
              (let [job (lottery/run-lottery @jobs)]
                (swap! jobs (partial filter (partial not= job)))
                (when (reserve-job errors/reserve-exception-handler conn job)
                  (->> (run-job config (d/db conn) job-handlers job)
                       (d/transact-async conn)))))))
      (timbre/info "Stopping job executor."))
    signal))
