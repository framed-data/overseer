(ns overseer.api
  "User-facing core API"
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [datomic.api :as d]
            (overseer
              [config :as config]
              [core :as core]
              [executor :as exc]
              [worker :as worker])))

(def default-config
  "Map of default configuration (connects to local Datomic)"
  config/default-config)

(def
  ^{:doc "Start the system as a library, given a map of
  {job-type job-handler}"
    :arglists '([config job-handlers])}
  start
  worker/start!)

(def
  ^{:doc "Construct a single unstarted job txn, given a type and optional
  arguments, asserted as attributes on the job entities
  Ex:
    (let [tx1 (job-txn :my-job-type-1)
          tx2 (job-txn :my-job-type-2 {:organization-id 123})]
      @(d/transact conn [tx1 tx2]))"
    :arglists '([job-type] [job-type args])}
  job-txn
  core/job-txn)

(defn graph-txns
  "Entry point to assert a sequence of jobs into the system.
   Given a job graph, and optional additional argument data,
   return a sequence of Datomic transactions that can be
   asserted directly

   Ex:
     (def txns (graph-txns
                 {:start []
                  :process-1a [:start]
                  :process-1b [:process-1a]
                  :process-2 [:start]
                  :finish [:process-1b :process-2]}
                 {:organization-id 123}))
     @(d/transact conn txns)"
  ([graph]
   (graph-txns graph {}))
  ([graph tx]
   (let [missing-deps (core/missing-dependencies graph)]
      (assert (empty? missing-deps)
              (str "Invalid graph; missing dependencies " (string/join ", " missing-deps)))
     (let [job-types (keys graph)
           jobs-by-type (core/job-txns-by-type job-types tx)
           dep-edges (core/job-dep-edges graph jobs-by-type)]
       (concat (vals jobs-by-type) dep-edges)))))

(defn validate-graph-handlers
  "Assert that a given graph only references handlers defined
   in the `handlers` map"
  [handlers graph]
  (let [missing-handlers (core/missing-handlers handlers graph)]
    (assert (empty? missing-handlers)
            (str "Invalid graph; missing handlers " (string/join ", " missing-handlers)))))

(defn abort
  "Control-flow helper to immediately mark a job as aborted from within a handler
   and abort all of its dependents (halts handler execution)
   Fails loudly by default - will log an error and and send to Sentry.
   See also `abort-silent`"
  [msg]
  (throw (ex-info msg {:overseer/status :aborted})))

(defn abort-silent
  "Like `abort`, but does not send exceptions to Sentry"
  [msg]
  (throw (ex-info msg {:overseer/status :aborted
                       :overseer/suppress? true})))

(defn fault
  "Signal that a transient fault has occurred and the worker should
   release and unstart the job so that it can be retried at a later time."
  [msg]
  (throw (ex-info msg {:overseer/status :unstarted
                       :overseer/suppress? true})))

(defn harness
  "A mechanism to 'wrap' job handlers, giving one the ability
   to provide additional context, inject dependencies, or otherwise
   alter the flow of execution. Conceptually, harnessing is similar to
   the idea behind Ring middleware.

   Accepts a standard job handler (map or function) and a
   `wrapper` function which will be called with the *function* specified
   in your handler and is expected to return a new *function* with the
   same signature. If your handler is a map, it will be transparently
   constructed/deconstructed; harnesses work solely in terms of functions.

   For example, a harness that simply implements the default behavior for
   a processor is:

     (defn my-harness [f]
       (fn [job]
         (f job)))

   A more substantive harness can be used to provide jobs with additional
   context or inject dependencies such as a database connection:

     (defn my-harness [f]
       (fn [job]
         (let [modified-job (assoc job :conn (d/connect my-datomic-uri))]
           (f modified-job))))

   Or add logging:

     (defn logging-harness [f]
       (fn [job]
         (println \"START execution of \" (:job/id job))
         (f job)
         (println \"FINISH execution of \" (:job/id job))))

   After defining a harness, in the job-handlers map one specifies

     {:my-job (overseer.api/harness my-job/run my-harness)}

   Following the previous example, within your handler you now have
   additional context available:

     (defn run [{:keys [conn] :as job}] ...)

   If your handler is a map, you can optionally specify a key to harness a
   specific stage; the default is :process. For example, to harness
   a post-processor:

     {:my-job (overseer.api/harness my-job/run :post-process my-harness)}

   If you attempt to harness a missing stage for a given job, the wrapper will
   be invoked with a properly-formed identity function, meaning you can write
   your harnesses in a single consistent fashion, and, for example, universally
   harness a post-processor for a set of handlers that may or may not define
   their own post-processor."
  ([handler wrapper]
   (harness handler :process wrapper))
  ([handler k wrapper]
   (let [id (if (= :post-process k)
              (fn [job res] job)
              (fn [job] job))]
     (if (map? handler)
      (update-in handler [k] (fn [f] (wrapper (or f id))))
      (harness {:process handler} k wrapper)))))
