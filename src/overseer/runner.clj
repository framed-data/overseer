(ns ^:no-doc overseer.runner
  (:gen-class))

(defn read-config [{:keys [config] :as options}]
  {:pre [config]}
  ((resolve 'clj-yaml.core/parse-string) (slurp (:config options))))

(defn parse-ns
  "Given a handler path like \"myapp.core/my-handlers\", parse it
   to the symbol 'myapp.core"
  [handler-path]
  (->> (butlast (clojure.string/split handler-path #"/"))
       first
       symbol))

(def cli-options
  [["-c" "--config PATH" "Path to YAML configuration file"
    :default nil]])

(defn print-usage []
  (let [{:keys [summary]} ((resolve 'clojure.tools.cli/parse-opts) [] cli-options)]
    (println "Usage: overseer.runner handlers [options]")
    (println summary)))

(defn -main [& args]
  ; Inline requires at runtime to avoid AOT compilation issues
  ; See https://github.com/onyx-platform/onyx/issues/339
  ; ("Side effect of AOT is that ALL namespaces that ns transitively depends on
  ;  are AOT-compiled, which breaks applications which use need different versions
  ;  of dependencies as AOT-compiled class files shadow all sources")
  (require '[clojure.tools.cli])
  (require '[clj-yaml.core])
  (require '[overseer.worker])

  (let [{:keys [options summary errors] :as opts}
        ((resolve 'clojure.tools.cli/parse-opts) args cli-options)]

    (if-not (.exists (clojure.java.io/file (:config options)))
      (do
        (print-usage)
        (System/exit 1)))

    (if-let [handlers-str (first (:arguments opts))]
      (do
        (require (parse-ns handlers-str))
        ((resolve 'overseer.worker/start!) (read-config options) (eval (symbol handlers-str))))
      (do
        (print-usage)
        (System/exit 1)))))
