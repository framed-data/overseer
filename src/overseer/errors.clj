(ns ^:no-doc overseer.errors
  "Functions for catching/handling various types of exceptions
   that arise during handler execution"
  (:require [clj-json.core :as json]
            (raven-clj
               [core :as raven]
               [interfaces :as raven.interface])
            [taoensso.timbre :as timbre]
            [overseer.config :as config])
  (:import java.util.concurrent.ExecutionException))

(defn try-thunk
  "Returns the value of calling f, or of calling exception-handler
   with any exception thrown"
  [exception-handler f]
  (try (f)
    (catch Throwable ex
      (exception-handler ex))))

(defn filter-serializable
  "Used for stripping out non-serializable fields
   before sending JSON over the wire to Sentry"
  [data]
  (let [safe?
        (fn [x]
          (try (json/generate-string x)
            (catch Exception ex false)))
        sanitize
        (fn [[k v]]
          (when (and (safe? k) (safe? v))
            [k v]))]
    (when data
      (->> (map sanitize data)
           (filter identity)
           (into {})))))

(defn- extract-ex-data
  "Like clojure.core/ex-data, but also works on arbitrarily nested
   java.util.concurrent.ExecutionException stack (threads can throw too)"
  [ex]
  (if (instance? ExecutionException ex)
    (extract-ex-data (.getCause ex))
    (ex-data ex)))

(defn sentry-capture
  "Send an exception and an optional map of additional context
   info to Sentry"
  [dsn ex extra]
  (let [event-map {:message (.getMessage ex) :extra (or extra {})}]
    (try
      (->> ex
           (raven.interface/stacktrace event-map)
           (raven/capture dsn))
      (catch Exception ex'
        (timbre/error "Sentry exception handler failed")
        (timbre/error ex')))))

(defn- ineligible-exception?
  "Was <ex> thrown by an ineligible job reservation?"
  [ex]
  (= :ineligible (:overseer/error (extract-ex-data ex))))

(defn reserve-exception-handler
  "Return nil in case of reservation failure, and re-throw
   all other errors"
  [ex]
  (if (ineligible-exception? ex)
    (do (timbre/warn (.getMessage ex)) nil)
    (do
      (timbre/error "Unexpected exception in reservation:")
      (timbre/error ex)
      (throw ex))))

(defn failure-info
  "Construct a map of information about an exception, including
   user-supplied ex-data if present"
  [ex]
  (let [exc-data (extract-ex-data ex)
        m {:overseer/status (or (:overseer/status exc-data) :failed)
           :overseer/failure {:reason :system/exception
                              :exception (class ex)
                              :message (.getMessage ex)}}]
    (if exc-data
      (assoc-in m [:overseer/failure :data] exc-data)
      m)))

(defn ->job-exception-handler
  "Exception handler for job thunks; log error then send to Sentry if configured
   Returns a map of failure info for consumption by worker"
  [config job]
  (if-let [dsn (config/sentry-dsn config)]
    (fn [ex]
      (timbre/error ex)
      (let [exc-data (extract-ex-data ex)]
        (when-not (:overseer/suppress? exc-data)
          (let [extra (merge (select-keys job [:job/type :job/id])
                             (or (filter-serializable exc-data) {}))]
            (sentry-capture dsn ex extra)))
        (failure-info ex)))

    (fn [ex]
      (timbre/error ex)
      (failure-info ex))))
