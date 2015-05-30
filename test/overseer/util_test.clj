(ns overseer.util-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [overseer.util :as util]))

(deftest test-try-thunk
  (timbre/with-log-level :report
    (let [safe-f (fn [] :ok)
          unsafe-f (fn [] (throw (Exception. "uh oh")))
          exception-handler (fn [_] :failed)]
      (is (= :ok (util/try-thunk exception-handler safe-f)))
      (is (= :failed (util/try-thunk exception-handler unsafe-f))))))

(deftest test-filter-serializable
  (let [ok {:foo 1 :bar "2"}
        bad {:quux 3 :norf (Object.)}]
    (is (= ok (util/filter-serializable ok)))
    (is (= {:quux 3} (util/filter-serializable bad)))))
