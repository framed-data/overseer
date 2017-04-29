(ns overseer.store.datomic-test
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [core :as core]
             [test-utils :as test-utils])
           [overseer.store.datomic :as store]
           [taoensso.timbre :as timbre]
           [clj-time.core :as tcore]
           [framed.std.time :as std.time]))

(defn test-datomic-conn []
  (d/connect (test-utils/bootstrap-datomic-uri)))

(deftest test-jobs-ready
  (let [conn (test-datomic-conn)
        db (d/db conn)
        ->squuid #(str (d/squuid))
        jobs-txn
        [{:db/id (d/tempid :db.part/user -1000)
          :job/id (->squuid)
          :job/status :unstarted}
         {:db/id (d/tempid :db.part/user -1001)
          :job/id (->squuid)
          :job/status :started}
         {:db/id (d/tempid :db.part/user -1002)
          :job/id (->squuid)
          :job/status :unstarted
          :job/dep [(d/tempid :db.part/user -1000)]}
         {:db/id (d/tempid :db.part/user -1003)
          :job/id (->squuid)
          :job/status :finished}
         {:db/id (d/tempid :db.part/user -1004)
          :job/id (->squuid)
          :job/status :unstarted
          :job/dep [(d/tempid :db.part/user -1003)]}
         {:db/id (d/tempid :db.part/user -1005)
          :job/id (->squuid)
          :job/status :unstarted
          :job/dep [(d/tempid :db.part/user -1000)
                    (d/tempid :db.part/user -1003)]}
         {:db/id (d/tempid :db.part/user -1006)
          :job/id (->squuid)
          :job/status :aborted}]
        {:keys [tempids db-after] :as txn} @(d/transact conn jobs-txn)
        resolve-tempid (fn [tempid]
                         (->> (d/tempid :db.part/user tempid)
                              (d/resolve-tempid db-after tempids)) )
        jobs-ready (store/jobs-ready' db-after)
        ready? (fn [ent-id]
                 (contains? jobs-ready (:job/id (d/entity db-after ent-id))))]
    (is (ready? (resolve-tempid -1000))
        "It finds :unstarted jobs with no dependencies")
    (is (not (ready? (resolve-tempid -1001)))
        "It excludes :started jobs")
    (is (not (ready? (resolve-tempid -1002)))
        "excludes jobs with unfinished dependencies")
    (is (not (ready? (resolve-tempid -1003)))
        "It excludes jobs that are already :finished")
    (is (ready? (resolve-tempid -1004))
        "It finds jobs where all dependencies are satisfied.")
    (is (not (ready? (resolve-tempid -1005)))
        "It excludes jobs where only some dependencies are satisfied.")
    (is (not (ready? (resolve-tempid -1006)))
        "It excludes jobs that are :aborted")))

(deftest test-transitive-dependents
  (let [conn (test-datomic-conn)
        graph [{:db/id (d/tempid :db.part/user -1001)
                :job/id "-1001"}
               {:db/id (d/tempid :db.part/user -1002)
                :job/id "-1002"
                :job/dep (d/tempid :db.part/user -1001)}
               {:db/id (d/tempid :db.part/user -1003)
                :job/id "-1003"
                :job/dep (d/tempid :db.part/user -1001)}
               {:db/id (d/tempid :db.part/user -1004)
                :job/id "-1004"
                :job/dep (d/tempid :db.part/user -1002)}
               {:db/id (d/tempid :db.part/user -1005)
                :job/id "-1005"}]
        {:keys [tempids db-after] :as txn} @(d/transact conn graph)]
    (is (= #{"-1002" "-1003" "-1004"}
           (store/transitive-dependents db-after "-1001"))
        "It returns all nodes that depend upon the given node, even indirectly.")
    (is (= #{} (store/transitive-dependents db-after "-1004"))
        "It doesn't return nodes that are only leaves")
    (is (= #{} (store/transitive-dependents db-after "-1005"))
        "It doesn't return disconnected nodes")))

(deftest test-updating-jobs
  (testing "reservation"
    (let [store (store/store (test-utils/bootstrap-datomic-uri))
          {job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (core/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (is (nil? (core/reserve-job store job-id))
          "It returns nil if it can't reserve an already started job")))

  (testing "finishing"
    (let [store (store/store (test-utils/bootstrap-datomic-uri))
          {job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (core/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (core/finish-job store job-id)
      (is (= :finished (:job/status (core/job-info store job-id))))))

  (testing "failing"
    (let [store (store/store (test-utils/bootstrap-datomic-uri))
          {job-id :job/id :as job} (test-utils/job {:job/type :foo})
          failure {:thing :went-wrong}]
      (core/transact-graph store (core/simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (core/fail-job store job-id failure)
      (is (= :failed (:job/status (core/job-info store job-id))))
      (is (= (pr-str failure) (:job/failure (core/job-info store job-id)))))))


(defn- unix<-millis-ago [now millis-ago]
  (->> (tcore/minus now (tcore/millis millis-ago))
       std.time/datetime->unix))

(deftest test-jobs-dead
 (timbre/with-log-level :report
    (let [store (store/store (test-utils/bootstrap-datomic-uri))
          now (tcore/now)

          ->job test-utils/job

          j1 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 1000)})
          j2 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 50000)}) ; Dead
          j3 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 500)})
          thresh (unix<-millis-ago now 3000)]
      (core/transact-graph store (core/simple-graph j1 j2 j3))
      (is (= [(:job/id j2)] (map :job/id (core/jobs-dead store thresh)))))))
