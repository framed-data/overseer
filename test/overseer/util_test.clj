(ns overseer.util-test
  (:require [clojure.test :refer :all]
            [overseer.util :as util]))

(deftest test-update
  (testing "when map contains key"
    (let [m {:foo 1}]
      (is (= {:foo 2} (util/when-update m :foo inc)))
      (is (= {:foo 3} (util/when-update m :foo + 2)))))
  (testing "when map does not contain key"
    (let [m {}]
      (is (= {} (util/when-update m :foo + 2))))))
