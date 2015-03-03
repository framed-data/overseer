(ns overseer.api-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [api :as api]
             [worker :as w])))

(deftest test-harness
  (let [state (atom 0)
        job {:foo "bar"}
        wrapper
        (fn [f]
          (fn [job]
            (swap! state inc)
            (f job)))]
    (testing "function handler"
      (let [handler (fn [job] (swap! state inc))
            harnessed (api/harness handler wrapper)]
        (reset! state 0)
        (w/invoke-handler harnessed job)
        (is (= 2 @state))))

    (testing "map handler"
      (let [handler {:process (fn [job] (swap! state inc))}
            harnessed (api/harness handler wrapper)]
        (reset! state 0)
        (w/invoke-handler harnessed job)
        (is (= 2 @state))))))
