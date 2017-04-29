(ns ^:no-doc overseer.store.jdbc
  "Internal core functions"
  (:require [clojure.java.jdbc :as j]
            [overseer.core :as overseer]
            [clojure.set :as set]
            (loom graph)))

;; (defrecord JdbcStore [db-spec]
;;   overseer/Store
;;   (reserve-job [this job-id]
;;
;;       @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :unstarted :started]
;;                          (heartbeat-assertion job-id)]))
;;
;;   (finish-job [this job-id]
;;     @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :started :finished]]))
;;
;;   (fail-job [this job-id failure]
;;     @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :started :failed]
;;                        [:db/add [:job/id job-id] :job/failure (pr-str failure)]]))
;;
;;   (heartbeat-job [this job-id]
;;     @(d/transact conn [(heartbeat-assertion job-id)]))
;;
;;   (abort-job [this job-id]
;;     (let [job-ids (cons job-id (transitive-dependents (d/db conn) job-id))
;;           abort (fn [jid]
;;                   [:db/add [:job/id jid] :job/status :aborted])]
;;       @(d/transact conn (map abort job-ids))))
;;
;;   (reset-job [this job-id]
;;     (let [old-heartbeat (:job/heartbeat (d/pull (d/db conn) [:job/heartbeat] [:job/id job-id]))]
;;       (with-ignore-cas
;;         @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/heartbeat old-heartbeat (heartbeat)]
;;                            [:db.fn/cas [:job/id job-id] :job/status :started :unstarted]]))))
;;
;;   (job-info [this job-id]
;;     (d/pull (d/db conn) [:*] [:job/id job-id]))
;;
;;   (transact-graph [this graph]
;;     (->> graph
;;          loom-graph->datomic-txn
;;          (d/transact conn)
;;          deref))
;;
;;   (jobs-ready [this]
;;     (jobs-ready' (d/db conn)))
;;   (jobs-dead [this thresh]
;;     (jobs-dead' (d/db conn) thresh)))
;;
;; (defn create-tables [db-spec]
;;   (j/db-do-commands
;;     db-spec
;;     (j/create-table-ddl
;;       :jobs
;;       [[:id "varchar(64) primary key"]
;;        [:status ]
;;        ]
;;
;;                         )
;;                     )
;;
;;
;;   )
;;
;; (defn store [db-spec]
;;   (->JdbcStore db-spec))
