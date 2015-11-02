(ns overseer.heartbeat-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as tcore]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [framed.std.time :as std.time]
            (overseer
              [heartbeat :as hb]
              [test-utils :as test-utils])))

(defn- unix<-millis-ago [now millis-ago]
  (->> (tcore/minus now (tcore/millis millis-ago))
       std.time/datetime->unix))

(deftest test-dead-jobs
 (timbre/with-log-level :report
    (let [conn (test-utils/connect)
          now (tcore/now)

          j1 (test-utils/->transact-job conn {:job/status :started
                                              :job/heartbeat (unix<-millis-ago now 1000)})
          j2 (test-utils/->transact-job conn {:job/status :started
                                              :job/heartbeat (unix<-millis-ago now 50000)}) ; Dead
          j3 (test-utils/->transact-job conn {:job/status :started
                                              :job/heartbeat (unix<-millis-ago now 500)})

          db (d/db conn)
          thresh (unix<-millis-ago now 3000)]
      (is (= [j2] (hb/dead-jobs db thresh))))))
