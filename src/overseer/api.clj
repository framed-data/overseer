(ns overseer.api
  "User-facing core API"
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [datomic.api :as d]
            (overseer
              [core :as core]
              [worker :as worker])))

(def default-config
  {:datomic {:uri "datomic:free://localhost:4334/overseer"}
   :sleep-time worker/default-sleep-time})

(def start
  "Alias in order to start the system as a library
  (start config job-handlers)"
  worker/start!)

(defn ->graph-txn
  "Entry point to add assert a sequence of jobs into the system.
   Given a job graph, and optional additional transaction data,
   return a full transaction for all nodes.

   Follow the strategy of:
     1. Assert all of the job nodes, one per type as described in
        the job graph.
     2. Assert off the dependency edges between the jobs nodes."
  ([graph]
   (->graph-txn graph {}))
  ([graph tx]
   (let [missing-deps (core/missing-dependencies graph)]
      (assert (empty? missing-deps)
              (str "Invalid graph; missing dependencies " (string/join ", " missing-deps)))
     (let [job-types (keys graph)
           jobs-by-type (core/job-assertions-by-type job-types tx)
           dep-edges (core/job-dep-edges graph jobs-by-type)]
       (concat
         (vals jobs-by-type)
         dep-edges)))))

(defn validate-graph-handlers [handlers graph]
  (let [missing-handlers (core/missing-handlers handlers graph)]
    (assert (empty? missing-handlers)
            (str "Invalid graph; missing handlers " (string/join ", " missing-handlers)))))

(defn abort-silent
  "Like `abort`, but does not send exceptions to Sentry"
  [msg]
  (throw (ex-info msg {:overseer/status :aborted
                       :overseer/suppress? true})))

(defn abort
  "Control-flow helper to mark a job as aborted from within a handler
   and abort all of its dependents (halts handler execution)
   Fails loudly by default - errors will be logged and sent to Sentry.
   Also see `abort-silent`"
  [msg]
  (throw (ex-info msg {:overseer/status :aborted})))

(defn harness
  "A mechanism to 'wrap' job handlers, giving one the ability
   to provide additional context prior to execution, somewhat similar
   to Ring middleware.

   Accepts a standard job handler (map or function) and a
   `wrapper` function which will be called with the *function* specified
   in your handler and is expected to return a new *function* with the
   same signature. If your handler is a map, it will be transparently
   constructed/deconstructed; harnesses work solely in terms of functions.

   For example, a harness that simply implements the default behavior is:

     (defn my-harness [f]
       (fn [job]
         (f job)))

   A more substantive harnesses can be used to provide jobs with additional
   context, for example a database connection:

     (defn my-harness [f]
       (fn [job]
         (-> job
             (assoc :conn (d/connect my-datomic-uri))
             (f))))

   In the job-handlers map, one specifies

     {:my-job (overseer.api/harness my-job/run my-harness)}

   Following the example, within your handler:

     (defn run [{:keys [conn] :as job}] ...)

   If your handler is a map, you can optionally specify a key to harness a
   specific stage; the default is :process. To harness a post-processor:

     {:my-job (overseer.api/harness my-job/run :post-process my-harness)}

   If you attempt to harness a missing key, the wrapper will be invoked with
   clojure.core/identity, meaning you can write your handlers in a single way, e.g.

     (defn my-harness [f]
       (fn [job]
         (f (assoc job :foo :bar))))

   and uniformly harness a set of handlers."
  ([handler wrapper]
   (harness handler :process wrapper))
  ([handler k wrapper]
   (let [id (if (= :post-process k)
              (fn [job res] job)
              (fn [job] job))]
     (if (map? handler)
      (update-in handler [k] (fn [f] (wrapper (or f id))))
      (harness {:process handler} k wrapper)))))
