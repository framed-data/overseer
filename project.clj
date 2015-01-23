(defproject overseer "0.1.0-SNAPSHOT"
  :description "A framework for defining and running expressive job pipelines"
  :url "https://github.com/framed-data/overseer"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [circleci/clj-yaml "0.5.3"]
                 [org.clojure/data.fressian "0.2.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [com.taoensso/timbre "3.2.1"]
                 [com.datomic/datomic-free "0.9.5130" :exclusions [joda-time]]
                 [raven-clj "1.1.0"]]
  :profiles {:uberjar {:aot :all}}
  :main overseer.system)
