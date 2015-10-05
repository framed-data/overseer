(ns ^:no-doc overseer.lottery
  "Lottery-style selection of ready jobs based on status
   Unstarted jobs are prioritized over started jobs, and thus
   receive more 'tickets'")

(defn job-tickets [{:keys [job/status] :as job}]
  (let [ntickets (condp = status
                   :unstarted 4
                   :started 1
                   1)]
    (repeat ntickets job)))

(defn generate-tickets [jobs]
  (mapcat job-tickets jobs))

(defn run-lottery
  "Run a lottery selection over a seq of jobs and return
   the winner"
  [jobs]
  (rand-nth (generate-tickets jobs)))
