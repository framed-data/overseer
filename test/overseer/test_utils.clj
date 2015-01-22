(ns overseer.test-utils
  "Helper functions for test suite"
  (:require [datomic.api :as d]
            [overseer.schema :as schema]))

(def test-config
  {:datomic {:uri "datomic:mem://overseer_test"}})

(def datomic-uri (get-in test-config [:datomic :uri]))

(defn refresh-database [datomic-uri]
  (d/delete-database datomic-uri)
  (d/create-database datomic-uri)
  (schema/install (d/connect datomic-uri)))
