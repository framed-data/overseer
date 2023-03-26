(ns overseer.api
  "User-facing core API

  Jobs are defined as maps with the following attributes:
    :job/id - Required String uniquely identifying a job
    :job/type - Keyword type name of this job
    :job/status - Keyword, one of #{:unstarted :started :finished :failed :aborted}
    :job/args - Optional arguments to the job (must be serializable as EDN)

  Jobs that have been run (or attempted) by the system may also have the following attributes:
    :job/heartbeat - Integer UNIX timestamp of the last time a worker processing
                     this job marked itself alive
    :job/failure - Map containing information about the failure of a job

  Graphs are defined as Loom digraphs where every node is a valid Job."
  (:require [clojure.set :as set]
            [clojure.string :as string]
            (overseer
              [config :as config]
              [core :as core]
              [executor :as exc]
              [worker :as worker])
            [overseer.store.datomic :as store.datomic]
            [overseer.store.jdbc :as store.jdbc]
            [loom.graph :as loom]))

(defn store
  "Return a Store implementation based on the store type and type-specific
  configuration in `config` map"
  [config]
  (case (config/store-type config)
    :datomic (store.datomic/store (:uri (config/datomic-config config)))
    :mysql (store.jdbc/store (config/jdbc-config config))
    :h2 (store.jdbc/store (config/jdbc-config config))
    :sqlite (store.jdbc/store (config/jdbc-config config))))

(defn start
  "Start the system inline given a config map, a Store implementation (see `store`)
  and a job-handler map of {job-type job-handler}"
  [config store job-handlers]
  (worker/start! config store job-handlers))

(defn job-graph
  "Given a map in Loom adjacency list format specifying dependencies between keyword job types,
  and an optional map of additional job data, return a Loom digraph of Job maps that can be
  transacted by a store. This assumes you are only generating one job per type.

  Ex:
    (def graph (job-graph
                {:start []
                 :process-1a [:start]
                 :process-1b [:process-1a]
                 :process-2 [:start]
                 :finish [:process-1b :process-2]}
                {:org/id 123}))
    ; => Graph<Job>, ex:
    ;    {{:job/id 1 :job/type :start :org/id 123} []
          {:job/id 2 :job/type :process-1a :org/id 123} [{:job/id 1 ...}]
          {:job/id 3 :job/type :process-1b :org/id 123} [{:job/id 2 ...}]
          ...}
    (core/transact-graph store txns graph)"
  ([job-type-graph]
   (job-graph job-type-graph {}))
  ([job-type-graph tx]
   (core/job-graph job-type-graph tx)))

(defn simple-graph
  "Construct a Graph from Job(s) that have no dependencies between them"
  [& jobs]
  (apply loom.graph/add-nodes (loom.graph/digraph) jobs))

(defn validate-graph-handlers
  "Assert that a given job-type keyword graph (see `job-graph`)
  only references handlers defined in the given `handlers` map."
  [handlers job-type-graph]
  (let [missing-handlers (core/missing-handlers handlers job-type-graph)]
    (assert (empty? missing-handlers)
            (str "Invalid graph; missing handlers " (string/join ", " missing-handlers)))))

(defn install
  "Install store configuration (create system tables, etc). *Not* guaranteed
  to be idempotent. Returns :ok on success"
  [store]
  (core/install store))

(defn transact-graph
  "Given a Graph, atomically transact all of its jobs/dependencies into
  the store. See `store`, `job-graph`."
  [store graph]
  (core/transact-graph store graph))

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
