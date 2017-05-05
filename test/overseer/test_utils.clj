(ns overseer.test-utils
  (:require [datomic.api :as d]
            loom.graph
            [framed.std.core :as std]
            [overseer.core :as core]
            [overseer.store.datomic :as store.datomic]))

(defn bootstrap-datomic-uri
  "Create/bootstrap a fresh memory DB and return its uri"
  []
  (let [uri (str "datomic:mem://" (std/rand-alphanumeric 32))]
    (d/create-database uri)
    (store.datomic/install' (d/connect uri))
    uri))

(defn store []
  (store.datomic/store (bootstrap-datomic-uri)))

(defn job
  ([]
   (job {}))
  ([attrs]
   (merge
     {:job/id (str (core/squuid))
      :job/type (keyword (std/rand-alphanumeric 16))
      :job/status :unstarted}
     attrs)))
