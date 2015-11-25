(def version (clojure.string/trim-newline (slurp "VERSION")))

(defproject io.framed/overseer version
  :description "A Clojure framework for defining and running expressive data pipelines"
  :url "https://github.com/framed-data/overseer"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git"
        :url "https://github.com/framed-data/overseer"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [circleci/clj-yaml "0.5.3"]
                 [org.clojure/data.fressian "0.2.0"]
                 [clj-json "0.5.3"]
                 [org.clojure/tools.cli "0.3.1"]
                 [com.taoensso/timbre "4.0.2"]
                 [com.datomic/datomic-free "0.9.5130" :exclusions [joda-time]]
                 [clj-http "1.1.2"]
                 [raven-clj "1.1.0"]
                 [io.framed/std "0.1.2"]]
  :aot [overseer.runner]
  :plugins [[codox "0.8.13"]])
