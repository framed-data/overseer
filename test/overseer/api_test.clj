(ns overseer.api-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [api :as api]
             [worker :as w])))

(deftest test-harness
  (let [state (atom 0)
        job {:foo "bar"}
        pre-process-wrapper
        (fn [f]
          (fn [job]
            (swap! state inc)
            (f job)))

        process-wrapper
        (fn [f]
          (fn [job args]
            (swap! state inc)
            (f job args)))]
    (testing "function handler"
      (let [handler (fn [job args]
                      (swap! state inc)
                      :quux)
            harnessed (api/harness handler process-wrapper)]
        (reset! state 0)
        (is (= :quux (w/invoke-handler harnessed job)))
        (is (= 2 @state))
        (is (map? harnessed))))

    (testing "mapping function handler"
      (let [handler (fn [job args]
                      (swap! state inc)
                      :quux)
            harnessed (api/harness handler :pre-process pre-process-wrapper)]
        (reset! state 0)
        (is (= :quux (w/invoke-handler harnessed job)))
        (is (= 2 @state))
        (is (= #{:pre-process :process} (set (keys harnessed))))))

    (testing "map handler - :process"
      (let [handler {:process (fn [job args] (swap! state inc))}
            harnessed (api/harness handler process-wrapper)]
        (reset! state 0)
        (w/invoke-handler harnessed job)
        (is (= 2 @state))))

    (testing "map handler - :pre-process"
      (let [handler {:pre-process (fn [job] (swap! state inc))
                     :process (fn [job args] job)}
            harnessed (api/harness handler :pre-process pre-process-wrapper)]
        (reset! state 0)
        (w/invoke-handler harnessed job)
        (is (= 2 @state))))

    (testing "map handler - :post-process"
      (let [post-wrapper
            (fn [f]
              (fn [job res]
                (swap! state inc)
                (f job res)))
            handler {:process (fn [job args] :quux)
                     :post-process (fn [job res]
                                     (swap! state inc)
                                     res)}
            harnessed (api/harness handler :post-process post-wrapper)]
        (reset! state 0)
        (is (= :quux (w/invoke-handler harnessed job)))
        (is (= 2 @state))))

    (testing "harnessing missing keys"
      (let [handler {:process (fn [job args] job)}
            post-wrapper (fn [f]
                           (fn [job res]
                             (swap! state inc)))
            post-harnessed (api/harness handler :post-process post-wrapper)]
        (reset! state 0)
        (is (= 0 @state))
        (w/invoke-handler post-harnessed job)
        (is (= 1 @state))))))
