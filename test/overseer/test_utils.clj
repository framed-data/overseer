(ns overseer.test-utils
  (:require [datomic.api :as d]
            loom.graph
            [framed.std.core :as std]
            [overseer.core :as core]
            [overseer.store.datomic :as store.datomic]
            [overseer.store.jdbc :as store.jdbc]))

(defn silent-cancel
  "Cancel future `fut`, suppressing any exceptions that occur as a result"
  [fut]
  (try (future-cancel fut) (catch Exception ex nil)))

(defn- bootstrap-datomic-uri
  "Create/bootstrap a fresh memory DB and return its uri"
  []
  (let [uri (str "datomic:mem://" (std/rand-alphanumeric 32))]
    (d/create-database uri)
    uri))

(defn datomic-store
  "Generate a fresh/configured Datomic-backed Store"
  []
  (let [store (store.datomic/store (bootstrap-datomic-uri))]
    (core/install store)
    store))

(defn jdbc-store
  "Generate a fresh/configured in-memory H2-backed Store"
  []
  (let [store
        (store.jdbc/store
          {:adapter :h2
           :db-spec
           {:classname "org.h2.Driver"
            :subprotocol "h2:mem://"
            :subname (str (std/rand-alphanumeric 12) ";DB_CLOSE_DELAY=-1")
            :user "sa" ; System Administrator username
            :password ""}})]
    (core/install store)
    store))

(defn job
  ([]
   (job {}))
  ([attrs]
   (merge
     {:job/id (str (core/squuid))
      :job/type (keyword (std/rand-alphanumeric 16))
      :job/status :unstarted}
     attrs)))
