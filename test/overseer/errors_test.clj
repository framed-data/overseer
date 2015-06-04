(ns overseer.errors-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [overseer.errors :as errors]))

(deftest test-try-thunk
  (timbre/with-log-level :report
    (let [safe-f (fn [] :ok)
          unsafe-f (fn [] (throw (Exception. "uh oh")))
          exception-handler (fn [_] :failed)]
      (is (= :ok (errors/try-thunk exception-handler safe-f)))
      (is (= :failed (errors/try-thunk exception-handler unsafe-f))))))

(deftest test-filter-serializable
  (let [ok {:foo 1 :bar "2"}
        bad {:quux 3 :norf (Object.)}]
    (is (= ok (errors/filter-serializable ok)))
    (is (= {:quux 3} (errors/filter-serializable bad)))))

(deftest test-job-exception-handler
  (timbre/with-log-level :report
    (let [ex (ex-info "uh oh" {:overseer/status :aborted})
          config {}
          job {:job/id -1 :job/type :foo}
          ex-handler (errors/->job-exception-handler config job)]
      (is (= :aborted (:overseer/status (ex-handler ex)))))))
