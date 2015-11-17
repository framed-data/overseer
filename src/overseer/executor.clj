(ns ^:no-doc overseer.executor
  "An Executor is the process that actually grabs a job
   and performs its work; thus, it is the real substance
   of a Worker."
  (:require [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [framed.std.core :refer [future-loop]]
            (overseer
              [config :as config]
              [core :as core]
              [lottery :as lottery]
              [errors :as errors])))

(defn reserve-job
  "Attempt to reserve a job and return it"
  [exception-handler conn job]
  {:pre [job]}
  (let [{:keys [job/id job/type]} job]
    (errors/try-thunk exception-handler
      (fn []
        (timbre/info (format "Reserving job %s (%s)" id type))
        @(d/transact conn [[:reserve-job id]])
        (timbre/info "Reserved job" id)
        job))))

(defn invoke-handler
  "Invoke a job handler, which can either be an ordinary function
   expecting a job argument, or a map of the following structure:

     ; Optional, runs prior to main processing function and can be
     ; used to set up prerequisite state, for example.
     :pre-process (fn [job] ...)

     ; Required, the main processing function.
     :process (fn [job] ...)

     ; Optional, for post-processing after main execution. Receives
     ; the job and return value of the :process function as arguments.
     :post-process (fn [job res] ...)"
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
    (when (= status :aborted)
      (timbre/info "Found :aborted job; aborting all dependents of" job-id))
    txns))

(defn start-executor
  "Construct an executor future that will perpetually run a scheduler over
   the `ready-jobs` atom to reserve and run jobs."
  [config conn job-handlers ready-jobs current-job]
  (future-loop
    (if (empty? @ready-jobs)
      (do (timbre/info "No handleable ready-jobs found. Waiting.")
          (Thread/sleep (config/sleep-time config)))
      (do (timbre/info "Found" (count @ready-jobs) "handleable jobs.")
          (let [{job-id :job/id :as job} (lottery/run-lottery @ready-jobs)]
            (swap! ready-jobs disj job)
            (when (reserve-job errors/reserve-exception-handler conn job)
              (reset! current-job job)
              (let [txns (run-job config (d/db conn) job-handlers job)]
                @(d/transact conn txns))
              (reset! current-job nil)))))))
