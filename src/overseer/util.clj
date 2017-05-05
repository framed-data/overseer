(ns overseer.util)

(defn when-update
  "Invoke (apply update m k f args) only if m contains k"
  [m k f & args]
  (if (contains? m k)
    (apply update m k f args)
    m))
