(ns ^:no-doc overseer.runner
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.string :as string]
            [clj-yaml.core :as yaml]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (overseer
              [schema :as schema]
              [worker :as worker]))
  (:gen-class))

(defn read-config [{:keys [config] :as options}]
  {:pre [config]}
  (yaml/parse-string (slurp (:config options))))

(defn parse-ns
  "Given a handler path like \"myapp.core/my-handlers\", parse it
   to the symbol 'myapp.core"
  [handler-path]
  (->> (butlast (string/split handler-path #"/"))
       first
       symbol))

(def cli-options
  [["-c" "--config PATH" "Path to YAML configuration file"
    :default nil]])

(defn print-usage []
  (let [{:keys [summary]} (cli/parse-opts [] cli-options)]
    (println "Usage: overseer.runner handlers [options]")
    (println summary)))

(defn -main [& args]
  (let [{:keys [options summary errors] :as opts}
        (cli/parse-opts args cli-options)]

    (if-not (.exists (io/file (:config options)))
      (do
        (print-usage)
        (System/exit 1)))

    (if-let [handlers-str (first (:arguments opts))]
      (do
        (require (parse-ns handlers-str))
        (worker/start! (read-config options) (eval (symbol handlers-str))))
      (do
        (print-usage)
        (System/exit 1)))))
