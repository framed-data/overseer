(ns overseer.util
  (:require [clj-json.core :as json]))

(defn try-thunk
  "Returns the value of calling f, or of calling exception-handler
   with any exception thrown"
  [exception-handler f]
  (try (f)
    (catch Throwable ex
      (exception-handler ex))))

(defn filter-serializable
  "Used for stripping out non-serializable fields
   before sending JSON to sentry"
  [data]
  (let [safe?
        (fn [x]
          (try (json/generate-string x)
            (catch Exception ex false)))
        sanitize
        (fn [[k v]]
          (when (and (safe? k) (safe? v))
            [k v]))]
    (->> (map sanitize data)
         (filter identity)
         (into {}))))

(defn sanitized-ex-data [ex]
  (when-let [data (ex-data ex)]
    (filter-serializable data)))
