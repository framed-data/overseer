(ns ^:no-doc overseer.executor
  "An Executor is the process that actually grabs a job
   and performs its work; thus, it is the real substance
   of a Worker."
  (:require [taoensso.timbre :as timbre]
            [framed.std.core :refer [future-loop]]
            (overseer
              [config :as config]
              [core :as core]
              [errors :as errors])))

(defn invoke-handler
  "Invoke a job handler, which can either be an ordinary function
   expecting a job argument, or a map of the following structure:

     ; Optional, runs prior to main processing function and can be
     ; used to ex: set up prerequisite state
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
        (pre-process job)
        (->> (process job)
             (post-process job)))
    (fn? handler)
      (handler job)
    :else
      (throw (Exception. "Handlers must either be a function or a map"))))

(defn run-job
  "Run a single job and return the appropriate status update txns"
  [config store job-handlers job]
  (let [{job-id :job/id
         job-type :job/type} job
        handler (get job-handlers job-type)
        _ (assert handler (str "Handler not specified: " job-type))

        {:keys [overseer/status] :as exit-status-map}
        (errors/try-thunk (errors/->job-exception-handler config job)
                          (fn []
                            (invoke-handler handler job)
                            {:overseer/status :finished}))]
    (timbre/info "Job" job-id "exited with status" status)
    (case status
      :finished (core/finish-job store job-id)
      :failed (core/fail-job store job-id (:overseer/failure exit-status-map))
      :aborted (core/abort-job store job-id)
      :unstarted (core/reset-job store job-id))
    exit-status-map))

(defn tick
  "Internal: Run a single 'tick' of the executor - attempt to reserve and run a job"
  [config store job-handlers ready-jobs current-job]
  (if (empty? @ready-jobs)
    (do (timbre/info "No handleable ready-jobs found. Waiting.")
        (Thread/sleep (config/sleep-time config)))
    (do (timbre/info "Found" (count @ready-jobs) "handleable jobs.")
        (let [{job-id :job/id :as job} (rand-nth (seq @ready-jobs))]
          (swap! ready-jobs disj job)

          (timbre/info (format "Reserving job %s (%s)" job-id (:job/type job)))
          (if-let [reserved-job (core/reserve-job store job-id)]
            (do (timbre/info "Reserved job" job-id)
                (reset! current-job job)
                (run-job config store job-handlers job)
                (reset! current-job nil))
            (timbre/info "Failed to reserve; skipping job" job-id))))))

(defn start-executor
  "Construct an executor future that will perpetually run a scheduler over
   the `ready-jobs` atom to reserve and run jobs."
  [ex-handler config store job-handlers ready-jobs current-job]
  (future-loop
    (errors/try-thunk
      ex-handler
      (fn [] (tick config store job-handlers ready-jobs current-job)))))
