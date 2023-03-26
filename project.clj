(def version (clojure.string/trim-newline (slurp "VERSION")))

(defproject io.framed/overseer version
  :description "A Clojure framework for defining and running expressive data pipelines"
  :url "https://github.com/framed-data/overseer"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git"
        :url "https://github.com/framed-data/overseer"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-json "0.5.3"]
                 [com.taoensso/timbre "4.0.2"]
                 [com.datomic/datomic-free "0.9.5130" :exclusions [joda-time]]
                 [raven-clj "1.5.2"]
                 [io.framed/std "0.2.5"]
                 [aysylu/loom "1.0.0"]
                 [org.clojure/java.jdbc "0.7.0-alpha3"]
                 [honeysql "0.8.2"]
                 [com.h2database/h2 "1.4.195"]
                 [com.velisco/herbert "0.7.0"]
                 [clj-time "0.12.2"]
                 [mysql/mysql-connector-java "5.1.41"]
                 [org.xerial/sqlite-jdbc "3.41.2.1"]]
  :plugins [[codox "0.8.13"]]
  :codox {:output-dir "doc/api"}
  :deploy-repositories [["clojars" {:sign-releases false :url "https://clojars.org/repo"}]])
