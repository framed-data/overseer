(ns overseer.store.jdbc-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [core :as core]
             [test-utils :as test-utils])
           [overseer.store.jdbc :as store]
           [taoensso.timbre :as timbre]
           [clj-time.core :as tcore]
           [framed.std.time :as std.time]))

(comment (defn test-datomic-conn []
  (d/connect (test-utils/bootstrap-datomic-uri))))
