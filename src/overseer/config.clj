(ns overseer.config)

(def default-config
  {:datomic {:uri "datomic:free://localhost:4334/overseer"}})

(defn datomic-uri [config]
  (let [uri (get-in config [:datomic :uri])]
    (assert uri "Datomic URI is required")
    uri))

(defn sleep-time [config]
  (get config :sleep-time 10000)) ; ms

(defn supervise? [config]
  (get config :supervise false))

(defn sentry-dsn [config]
  (get-in config [:sentry :dsn]))
