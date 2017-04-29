(ns overseer.test-utils
  (:require [datomic.api :as d]
            [framed.std.core :as std]
            (overseer
              [core :as overseer]
              [schema :as schema])
            [overseer.store.datomic :as datomic-store]
            [loom.graph :as loom]))

(defn bootstrap-datomic-uri
  "Create/bootstrap a fresh memory DB and return its uri"
  []
  (let [uri (str "datomic:mem://" (std/rand-alphanumeric 32))]
    (d/create-database uri)
    (schema/install (d/connect uri))
    uri))

(defn store []
  (datomic-store/store (bootstrap-datomic-uri)))

(defn job
  ([]
   (job {}))
  ([attrs]
   (merge
     {:job/id (str (overseer/squuid))
      :job/status :unstarted}
     attrs)))
