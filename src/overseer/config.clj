(ns overseer.config
  "Functions for reading Overseer configuration. Many configuration values are
  optional, and Overseer should 'just work' out of the box. The entire set
  of configuration options are:

    :store - Required backend store type to connect to. For now, must be :datomic
             The corresponding key of the same name must also be present
             to configure the store

    :datomic - map configuring the Datomic store (required if using Datomic store)
      :uri - If using Datomic store, required connection String, ex:
             \"datomic:free://localhost:4334/overseer\"

  :sentry - Optional map configuring the Sentry error backend
    :dsn - String DSN to use

  :detector-sleep-time - Optional; How long to sleep between ready job detector runs in ms
                         (default: 2000)

  :sleep-time - Optional; How long to sleep in ms if the job queue is empty (default: 10000)

  :heartbeat - map of optional attributes to configure worker heartbeating
    :enabled - When enabled, each node will periodically persist a timestamp
               'heartbeat' via the DB and also act as a monitor resetting jobs
               detected to be failing heartbeat checks
               (default: true)
    :sleep-time - How long to sleep in ms before persisting heartbeat (per-worker)
                  (default: 60000)
    :tolerance - How many heartbeats can fail before job is considered dead
                 to be reset by a monitor (default: 5)")

(defn store-type [config]
  (let [store-type (some-> config :store keyword)]
    (assert (and store-type (contains? config store-type))
            "Store type and corresponding config key is required")
    store-type))

(defn datomic-config [config]
  (let [cfg (:datomic config)]
    (assert (:uri cfg) "Datomic URI is required")
    cfg))

(defn detector-sleep-time [config]
  (get config :detector-sleep-time 2000))

(defn sleep-time [config]
  (get config :sleep-time 10000))

(defn heartbeat?  [config]
  (get-in config [:heartbeat :enabled] true))

(defn heartbeat-sleep-time [config]
  (get-in config [:heartbeat :sleep-time] 60000))

(defn failed-heartbeat-tolerance [config]
  (get-in config [:heartbeat :tolerance] 5))

(defn sentry-dsn [config]
  (get-in config [:sentry :dsn]))
