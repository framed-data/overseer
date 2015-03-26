(ns overseer.priority)

(defn- compare-by-date [a b]
  "Compare jobs a and b by date, prioritizing older jobs"
  (let [date-a (:job/created-at a)
        date-b (:job/created-at b)]
    (assert date-a (str ":job/created-at not specified for: " (:job/id a)))
    (assert date-b (str ":job/created-at not specified for: " (:job/id b)))
    (compare date-a date-b)))

(defn- compare-by-priority
  "Compare jobs a and b, each with explicitly specified priority,
   and fall back to a date comparison if priorities are equal"
  [a b]
  (let [prio-a (:job/priority a)
        prio-b (:job/priority b)]
    (assert prio-a (str ":job/created-at not specified for: " (:job/id a)))
    (assert prio-b (str ":job/created-at not specified for: " (:job/id b)))
    (let [res (compare prio-a prio-b)]
      (if (zero? res)
        (compare-by-date a b)
        res))))

(defn priority-compx
  "Comparator to sort jobs in *descending* priority order, highest priority first.
   Priority can either be set manually using the :job/priority attribute
   (lower numbers are higher priority, 0 being top priority), otherwise sort by
   date created, prioritizing the oldest jobs"
  [a b]
  (let [prio-a (:job/priority a)
        prio-b (:job/priority b)]
    (cond
      (and prio-a prio-b) (compare-by-priority a b)
      prio-a -1
      prio-b 1
      :else (compare-by-date a b))))

(defn select-job [jobs]
  (first (sort-by priority-compx jobs)))
