(ns overseer.priority-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (overseer
              [test-utils :as test-utils]
              [priority :as p])))

(deftest test-priority-compx
  (testing "when priority is specified"
    (let [j1 {:job/priority 0 :job/created-at (test-utils/inst 2015 3 22 9 0 0)}
          j2 {:job/priority 5 :job/created-at (test-utils/inst 2015 3 22 8 0 0)}
          j3 {:job/priority 10 :job/created-at (test-utils/inst 2015 3 22 7 0 0)}]
      (is (= [j1 j2 j3] (sort p/priority-compx [j3 j2 j1])))))

  (testing "when priority is partially specified"
    (let [j1 {:job/priority 5 :job/created-at (test-utils/inst 2015 3 22 8 0 0)}
          j2 {:job/created-at (test-utils/inst 2015 3 22 7 0 0)}
          j3 {:job/created-at (test-utils/inst 2015 3 22 8 0 0)}]
      (is (= [j1 j2 j3] (sort p/priority-compx [j3 j2 j1])))))

  (testing "when priority is not specified"
    (let [j1 {:job/created-at (test-utils/inst 2015 3 22 7 0 0)}
          j2 {:job/created-at (test-utils/inst 2015 3 22 8 0 0)}
          j3 {:job/created-at (test-utils/inst 2015 3 22 9 0 0)}]
      (is (= [j1 j2 j3] (sort p/priority-compx [j3 j2 j1])))))

  (testing "with equal priorities"
    (let [j1 {:job/priority 0 :job/created-at (test-utils/inst 2015 3 22 7 0 0)}
          j2 {:job/priority 0 :job/created-at (test-utils/inst 2015 3 22 8 0 0)}
          j3 {:job/priority 1 :job/created-at (test-utils/inst 2015 3 22 6 0 0)}
          j4 {:job/priority 2 :job/created-at (test-utils/inst 2015 3 22 10 0 0)}]
      (is (= [j1 j2 j3 j4] (sort p/priority-compx [j4 j3 j2 j1]))))))
