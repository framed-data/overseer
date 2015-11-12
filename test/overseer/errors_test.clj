(ns overseer.errors-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [overseer.errors :as errors])
  (:import java.util.concurrent.ExecutionException))

(deftest test-try-thunk
  (timbre/with-log-level :report
    (let [safe-f (fn [] :ok)
          unsafe-f (fn [] (throw (Exception. "uh oh")))
          exception-handler (fn [_] :failed)]
      (is (= :ok (errors/try-thunk exception-handler safe-f)))
      (is (= :failed (errors/try-thunk exception-handler unsafe-f))))))

(deftest test-failure-info
  (testing "It extracts status info from IExceptionInfo"
    (let [ex (ex-info "" {:overseer/status :aborted})]
      (is (= :aborted (:overseer/status (errors/failure-info ex))))))
  (testing "It extracts status info from IExceptionInfo in (nested-)threads"
    (let [exc-data {:overseer/status :aborted}
          ex (ExecutionException.
               (ExecutionException. (ex-info "boom" exc-data)))]
      (is (= :aborted (:overseer/status (errors/failure-info ex)))))))

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
