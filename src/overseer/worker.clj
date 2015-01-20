(ns overseer.worker
  (:require [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (overseer
              [core :as core]
              [status :as status])))

(def signal (atom nil))

(defn signal! [sig status]
  (reset! sig status))

(defn try-thunk [exception-handler fallback-value f]
  (try (f)
    (catch Throwable ex
      (exception-handler ex)
      fallback-value)))

; TODO: why return vectors in jobs? Doesn't seem used
(defn job-exit-status [[status & _]]
  (get #{:aborted :finished :failed} status :finished))

; TODO: filter is to only select jobs of type that are defined in the handler map?
; Seems overly cautious
(defn ready-job-entites [db job-handlers]
  (->> (status/jobs-ready db)
       (map (partial core/->job-entity db))
       (filter (comp job-handlers :job/type))))

(defn update-job-status
  "Transact a status update(s) for a completed job
   If job was aborted, then also abort all its dependents"
  [conn job status]
  (let [db (d/db conn)
        job-aborted (= :aborted status)
        job-id (:job/id job)
        job-ids (if job-aborted
                  (conj (core/transitive-dependents db job-id) job-id)
                  [job-id])]
    (timbre/info "Exited job" job-id "with status" status)
    (when job-aborted
      (timbre/info "Found :aborted job; aborting all dependents of" job-id))
    (d/transact conn (map (partial core/status-txn status) job-ids))))

(defn reserve-job [exception-handler conn job]
  (let [job-ent-id (:db/id job)
        job-id (:job/id job)]
    (try-thunk exception-handler nil
      #(do (timbre/info "Reserving job" job-id)
           (core/reserve conn job-ent-id)
           (timbre/info "Reserved job" job-id)
           job))))

(defn ->job-executor
  "Construct a function that will reserve a job off the queue and execute it

   state-provider is a nullary function that returns a worker state
   suitable for running job handlers etc; i.e. it provides {:config <conf>, ...}"
  [config exception-handler state-provider job-handlers]
  (fn []
    (let [conn (d/connect (get-in config [:datomic :uri]))
          jobs (ready-job-entites (d/db conn) job-handlers)]
      (when-not (empty? jobs)
        (timbre/info (count jobs) "handleable job(s) found.")
        (let [job (rand-nth jobs)
              ; TODO: (select-job db) ?
              ; job-id (rand-nth (status/jobs-ready db))
              ; job (core/->job-entity db job-id)
              job-ent-id (:db/id job)
              job-handler (get job-handlers (:job/type job))]
          (when (reserve-job exception-handler conn job)
            (let [state (state-provider)
                  status (try-thunk exception-handler :failed
                           ; TODO: could pass whole job here?
                           ; or just :job/id?
                           #(-> (job-handler state job-ent-id)
                                (job-exit-status)))]
              (update-job-status conn job status))))))))

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
        (if (= @signal :stop-when-empty)
          (timbre/info "Empty worker queue and `:stop-when-empty` set; stopping.")
          (do (timbre/info "No handleable jobs found.  Waiting.")
              (Thread/sleep sleep-time)
              (recur)))))))

(defn start! [worker-state state-provider exception-handler job-handlers signal]
  (timbre/info "Worker starting!")
  (let [config (:config worker-state)
        job-executor (->job-executor config exception-handler state-provider job-handlers)
        sleep-time (get config :sleep-time)]
    (try (run job-executor signal sleep-time)
      (catch Exception ex
        (exception-handler ex)))))

(defn stop! [signal]
  (signal! signal :stop))

(defn stop-when-empty! [signal]
  (signal! signal :stop-when-empty))
