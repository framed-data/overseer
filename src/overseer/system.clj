(ns overseer.system
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as string]
            [clj-yaml.core :as yaml]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            (raven-clj
               [core :as raven]
               [interfaces :as raven.interface])
            (overseer
              [schema :as schema]
              [worker :as worker])
            ))



; TODO: not sure about this
; Technically we could probably statically treat job-handlers as a symbol,
; but that's jank and wouldn't work for state provider
; state provider is also jank - could leave that as a user concern?
;(def dynamic-config (atom {:job-handlers nil
;                           :state-provider identity}))
;
;; TODO: not sure about this
;(defn configure
;  "Set the runtime configuration of the system, e.g.
;   specify the job handler entry points into application code"
;  [{:keys [job-handlers state-provider] :as config}]
;  {:pre [job-handlers]}
;  (swap! system/dynamic-config merge config))





(def default-static-config
  {:datomic {:uri "datomic:free://localhost:4334/overseer"}
   :sleep-time 10000})

(defn ->static-config [config-path]
  (if config-path
    (merge default-static-config
           (yaml/parse-string (slurp config-path)))
    (do
      (timbre/warn "Warning: no config specified, using default options")
      default-static-config)))

(defn ->system [options]
  (let [config (->static-config (:config options))
        datomic-uri (get-in config [:datomic :uri])]
    {:config config
     :conn (d/connect datomic-uri)}))

(defn default-exception-handler
  "Return an exception handler function that logs errors and optionally
   sends to Sentry if configured"
  [config]
  (let [capture
        (fn [dsn ex]
          (let [ex-map
                (-> {:message (.getMessage ex)}
                    (assoc :extra (or (ex-data ex) {}))
                    (raven.interface/stacktrace ex))]
            (try (raven/capture dsn ex-map)
              (catch Exception ex'
                (timbre/error "Senry exception handler failed")
                (timbre/error ex')))))

        dsn (get-in config [:sentry :dsn])]
    (fn [ex]
      (timbre/error ex)
      (when dsn
        (capture dsn ex)))))


; TODO: state provider??
(defn start [job-handlers {:keys [config] :as system}]
  (let [worker-signal (atom :start)
        state-provider identity
        exception-handler (default-exception-handler config)]
    (-> system
        (assoc-in [:worker :signal] worker-signal)
        (assoc-in [:worker :ref] (future
                                   (worker/start!
                                     system
                                     state-provider
                                     exception-handler
                                     job-handlers
                                     worker-signal))))))



(defn deref-worker [system]
  (if-let [worker-ref (get-in system [:worker :ref])]
    @worker-ref
    (timbre/warn "Attempted to deref a nil worker")))

(defn stop [system]
  (worker/stop! (get-in system [:worker :signal]))
  (timbre/info "Signalled system stop; waiting for stop.")
  (deref-worker system))

(defn stop-when-empty [system]
  (worker/stop-when-empty! (get-in system [:worker :signal]))
  (timbre/info "Signalled system stop-when-empty; waiting for stop.")
  (deref-worker system))

(defn parse-ns
  "Given a handler path like \"myapp.core/my-handlers\", parse it
   to the symbol 'myapp.core"
  [handler-path]
  (->> (butlast (string/split handler-path #"/"))
       first
       symbol))

(def cli-options
  [["-c" "--config PATH" "Path to YAML configuration file"
    :default nil]
   ["-h" "--help" "Display this menu and exit"]])

(defn print-usage []
  (let [{:keys [summary]} (cli/parse-opts [] cli-options)]
  (println "Usage: overseer.system handlers [options]")
  (println summary)))

(defn -main
  [& args]
  (let [{:keys [options summary errors] :as opts}
        (cli/parse-opts args cli-options)]
    (when (:help options)
      (print-usage)
      (System/exit 0))

    (if-let [handlers-str (first (:arguments opts))]
      (do
        (require (parse-ns handlers-str))
        (start (eval (symbol handlers-str)) (->system options)))
      (do
        (print-usage)
        (System/exit 1)))))

    ;(when (= ":stop-when-empty" (first args))
    ;  (stop-when-empty system)
    ;  (d/shutdown false)
    ;  (shutdown-agents)
    ;  (System/exit 0))
