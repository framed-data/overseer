(ns overseer.api-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           [taoensso.timbre :as timbre]
           (overseer
             [api :as api]
             [test-utils :as test-utils]
             [executor :as exc])))

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
        (is (= :quux (exc/invoke-handler harnessed job)))
        (is (= 2 @state))
        (is (map? harnessed))))

    (testing "mapping function handler"
      (let [handler (fn [job]
                      (swap! state inc)
                      :quux)
            harnessed (api/harness handler :pre-process wrapper)]
        (reset! state 0)
        (is (= :quux (exc/invoke-handler harnessed job)))
        (is (= 2 @state))
        (is (= #{:pre-process :process} (set (keys harnessed))))))

    (testing "map handler - :process"
      (let [handler {:process (fn [job] (swap! state inc))}
            harnessed (api/harness handler wrapper)]
        (reset! state 0)
        (exc/invoke-handler harnessed job)
        (is (= 2 @state))))

    (testing "map handler - :pre-process"
      (let [handler {:pre-process (fn [job] (swap! state inc))
                     :process (fn [job] job)}
            harnessed (api/harness handler :pre-process wrapper)]
        (reset! state 0)
        (exc/invoke-handler harnessed job)
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
        (is (= :quux (exc/invoke-handler harnessed job)))
        (is (= 2 @state))))

    (testing "harnessing missing keys"
      (let [handler {:process (fn [job] (:foo job))}
            pre-wrapper (fn [f]
                          (fn [job]
                            (f (assoc job :foo :bar))))
            post-wrapper (fn [f]
                           (fn [job res]
                             (swap! state inc)))
            pre-harnessed (api/harness handler :pre-process pre-wrapper)
            post-harnessed (api/harness handler :post-process post-wrapper)]
        (reset! state 0)
        (is (= :bar (exc/invoke-handler pre-harnessed job)))
        (is (= 0 @state))
        (exc/invoke-handler post-harnessed job)
        (is (= 1 @state))))))

(deftest test-fault
  (timbre/with-log-level :report
    (let [config {}
          conn (test-utils/connect)
          job-ran? (atom false)
          job-handlers {:bar (fn [job]
                               (reset! job-ran? true)
                               (api/fault "transient problem occurred"))}
          job (test-utils/->transact-job conn {:job/type :bar})
          job-ent-id (:db/id job)
          status-txns (exc/run-job config conn job-handlers job)
          {:keys [db-before db-after]} @(d/transact conn status-txns)]
      (is (= :unstarted (:job/status (d/entity db-before job-ent-id))))
      (is @job-ran?)
      (is (= :unstarted (:job/status (d/entity db-after job-ent-id)))))))
