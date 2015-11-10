(ns overseer.config)

(def default-config
  {:datomic {:uri "datomic:free://localhost:4334/overseer"}})

(defn datomic-uri [config]
  (let [uri (get-in config [:datomic :uri])]
    (assert uri "Datomic URI is required")
    uri))

(defn sleep-time
  "How long to sleep in ms if the job queue is empty"
  [config]
  (get config :sleep-time 10000))

(defn heartbeat?
  "When enabled, each node will periodically persist a timestamp
   'heartbeat' via the DB and also act as a monitor resetting jobs
   detected to be failing heartbeat checks"
  [config]
  (get-in config [:heartbeat :enabled] true))

(defn heartbeat-sleep-time
  "How long to sleep in ms before persisting heartbeat (per-worker)"
  [config]
  (get-in config [:heartbeat :sleep-time] 60000))

(defn failed-heartbeat-tolerance
  "How many heartbeats can fail before job is considered dead
   to be reset by a monitor"
  [config]
  (get-in config [:heartbeat :tolerance] 5))

(defn sentry-dsn [config]
  (get-in config [:sentry :dsn]))
