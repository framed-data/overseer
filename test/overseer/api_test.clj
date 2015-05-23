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
      (let [handler (fn [job]
                      (swap! state inc)
                      :quux)
            harnessed (api/harness handler wrapper)]
        (reset! state 0)
        (is (= :quux (w/invoke-handler harnessed job)))
        (is (= 2 @state))
        (is (map? harnessed))))

    (testing "mapping function handler"
      (let [handler (fn [job]
                      (swap! state inc)
                      :quux)
            harnessed (api/harness handler :pre-process wrapper)]
        (reset! state 0)
        (is (= :quux (w/invoke-handler harnessed job)))
        (is (= 2 @state))
        (is (= #{:pre-process :process} (set (keys harnessed))))))

    (testing "map handler - :process"
      (let [handler {:process (fn [job] (swap! state inc))}
            harnessed (api/harness handler wrapper)]
        (reset! state 0)
        (w/invoke-handler harnessed job)
        (is (= 2 @state))))

    (testing "map handler - :pre-process"
      (let [handler {:pre-process (fn [job] (swap! state inc))
                     :process (fn [job] job)}
            harnessed (api/harness handler :pre-process wrapper)]
        (reset! state 0)
        (w/invoke-handler harnessed job)
        (is (= 2 @state))))

    (testing "map handler - :post-process"
      (let [post-wrapper
            (fn [f]
              (fn [job res]
                (swap! state inc)
                (f job res)))
            handler {:process (fn [job] :quux)
                     :post-process (fn [job res]
                                     (swap! state inc)
                                     res)}
            harnessed (api/harness handler :post-process post-wrapper)]
        (reset! state 0)
        (is (= :quux (w/invoke-handler harnessed job)))
        (is (= 2 @state))))

    (testing "harnessing missing keys"
      (let [handler {:process (fn [job] (:foo job))}
            pre-wrapper (fn [f]
                          (fn [job]
                            (f (assoc job :foo :bar))))
            post-wrapper (fn [f]
                           (fn [job res]
                             (f (assoc job :foo :quux) res)))
            pre-harnessed (api/harness handler :pre-process pre-wrapper)
            post-harnessed (api/harness handler :post-process post-wrapper)]
        (is (= :bar (w/invoke-handler pre-harnessed job)))
        (is (= {:foo :quux} (w/invoke-handler post-harnessed job)))))))
