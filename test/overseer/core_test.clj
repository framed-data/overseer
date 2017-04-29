(ns overseer.core-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           loom.graph
           (overseer
             [core :as core]
             [test-utils :as test-utils])))

(deftest test-missing-handlers
  (let [handlers {:foo (fn [_] nil)
                  :bar (fn [_] nil)}
        g1 {:foo []
            :bar [:foo]}

        g2 {:foo []
            :quux [:foo]}]
    (is (= #{} (core/missing-handlers handlers g1)))
    (is (= #{:quux} (core/missing-handlers handlers g2)))))
