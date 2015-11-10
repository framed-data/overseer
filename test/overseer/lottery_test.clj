(ns overseer.lottery-test
  (:require [clojure.test :refer :all]
            [overseer.lottery :as lottery]))

(deftest test-generate-tickets
  (let [j1 {:job/status :unstarted :job/id 1}
        j2 {:job/status :unstarted :job/id 2}
        j3 {:job/status :unstarted :job/id 3}]
    (is (= [j1 j2 j3]
           (lottery/generate-tickets [j1 j2 j3])))))
