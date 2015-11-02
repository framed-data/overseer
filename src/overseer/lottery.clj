(ns ^:no-doc overseer.lottery
  "Lottery-style selection of ready jobs based on status

   NOTE: While this may seem superfluous, future work may
   make this more relevant")

(defn job-tickets [{:keys [job/status] :as job}]
  (let [ntickets (condp = status
                   :unstarted 1
                   1)]
    (repeat ntickets job)))

(defn generate-tickets [jobs]
  (mapcat job-tickets jobs))

(defn run-lottery
  "Run a lottery selection over a seq of jobs and return
   the winner"
  [jobs]
  (rand-nth (generate-tickets jobs)))
