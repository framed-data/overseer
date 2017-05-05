(ns overseer.core-test
 (:require [clojure.test :refer :all]
           [clj-time.core :as clj-time]
           [datomic.api :as d]
           loom.graph
           (overseer
             [core :as core]
             [test-utils :as test-utils])))

(deftest test-valid-job?
  (is (core/valid-job?
        {:job/id "123"
         :job/type :intake
         :job/status :started}))
  (is (core/valid-job?
        {:job/id "123"
         :job/type :intake
         :job/status :unstarted
         :job/heartbeat 12345}))
  (is (not (core/valid-job?
        {; No :job/id
         :job/type :intake
         :job/status :unstarted}))))

(deftest test-valid-graph?
  (testing "when graph is well-formed"
    (let [->job (fn [job-type]
                  {:job/id (str (java.util.UUID/randomUUID))
                   :job/type job-type
                   :job/status :unstarted})
          start-job (->job :start)
          result1-job (->job :result1)
          result2-job (->job :result2)
          finish-job (->job :finish)

          graph
          (loom.graph/digraph
            {start-job []
             result1-job [start-job]
             result2-job [start-job]
             finish-job [result1-job result2-job]})]
      (is (core/valid-graph? graph))))
  (testing "when graph does not satisfy Digraph interface"
    (let [j0 (test-utils/job {:job/type :start})
          j1 (test-utils/job {:job/type :step1})
          j2 (test-utils/job {:job/type :step2})

          ; Structurally this graph is well formed, but it does not satisfy
          ; `loom.graph/Digraph` (missing `loom.graph/digraph` call)
          map-graph
          {j0 []
           j1 [j0]
           j2 [j1]}]
      (is (not (core/valid-graph? map-graph)))))
  (testing "when graph contains invalid nodes"
    (let [j0 {:job/type :start} ; No :job/id or other required fields
          graph (loom.graph/digraph {j0 []})]
      (is (not (core/valid-graph? graph))))))

(deftest test-missing-handlers
  (let [handlers {:foo (fn [_] nil)
                  :bar (fn [_] nil)}
        g1 {:foo []
            :bar [:foo]}

        g2 {:foo []
            :quux [:foo]}]
    (is (= #{} (core/missing-handlers handlers g1)))
    (is (= #{:quux} (core/missing-handlers handlers g2)))))
