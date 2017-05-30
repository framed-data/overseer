(ns overseer.config
  "Functions for reading Overseer configuration. Many configuration values are
  optional, and Overseer should 'just work' out of the box. The entire set
  of configuration options are:

    :store
      :adapter - Required String backend store type to connect to. One of datomic, mysql, h2
      :config - options to configure the selected adapter
        If Datomic:
          - map of :uri, required connection String, ex: \"datomic:free://localhost:4334/overseer\"
        If MySQL/H2:
          - clojure.java.jdbc \"database spec\"
            - map of :dbtype, :dbname and other options as needed, such as :user, :password, :host, :port
            - map of driver :classname, :subprotocol, :hostname, :port, :subname, :user, :password
            - map of :connection-uri String option passed directly to driver
            - map of :name and :environment keys for JNDI connection
            - Can also just be a String JDBC URI
            See http://clojure-doc.org/articles/ecosystem/java_jdbc/home.html#setting-up-a-data-source

    :sentry - Optional map configuring the Sentry error backend
      :dsn - String DSN to use

    :detector-sleep-time - Optional; How long to sleep between ready job detector runs in ms
                           (default: 2000)

    :sleep-time - Optional; How long to sleep in ms if the job queue is empty (default: 10000)

    :heartbeat - map of optional attributes to configure worker heartbeating
      :sleep-time - How long to sleep in ms before persisting heartbeat (per-worker)
                    (default: 60000)
      :tolerance - How many heartbeats can fail before job is considered dead
                 to be reset by a monitor (default: 5)

      :monitor-shutdown - Boolean, whether to shutdown the system when the heartbeat
                          monitor encounters an error (avoids running in a degraded state)
                          Default: true"
  (:require [framed.std.core :as std]))

(defn store-type [config]
  (let [adapter (some-> config :store :adapter keyword)]
    (assert adapter "Store adapter is required")
    adapter))

(defn datomic-config [config]
  (let [cfg (get-in config [:store :config])]
    (assert (:uri cfg) ":uri is required when using Datomic")
    cfg))

(def jdbc-adapters #{:mysql :h2})

(defn jdbc-config [config]
  (let [db-spec (get-in config [:store :config])
        adapter (store-type config)]
    (assert (and db-spec (contains? jdbc-adapters adapter))
            "Valid JDBC adapter and config are required")
    (std/map-from-keys adapter db-spec)))

(defn detector-sleep-time [config]
  (get config :detector-sleep-time 2000))

(defn sleep-time [config]
  (get config :sleep-time 10000))

(defn heartbeat-sleep-time [config]
  (get-in config [:heartbeat :sleep-time] 60000))

(defn failed-heartbeat-tolerance [config]
  (get-in config [:heartbeat :tolerance] 5))

(defn monitor-shutdown? [config]
  (get-in config [:heartbeat :monitor-shutdown] true))

(defn sentry-dsn [config]
  (get-in config [:sentry :dsn]))
