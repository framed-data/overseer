# Starting a Worker

Once you've specified your dependencies in a job graph and corresponding handler functions, you're ready to start a worker, which is simply a single instance of Overseer. Once started, workers will perpetually check for eligible jobs to run and execute them, or sleep for a bit before checking again if no eligible jobs are found.

Overseer tries to provide very little 'magic' around your handler functions, but it will catch any exceptions that may be thrown in your handlers and update job statuses as appropriate. If an exception is thrown within a handler for a job, that job will be marked as `:failed` and Overseer will select another job to execute; otherwise successful jobs are marked as `:finished` and the cycle continues.

You start Overseer by including it in your code in a desired main namespace and calling a start function. Here's an example (see previous sections for a more full discussion of how job graphs and handlers are specified)

```clj
(ns myapp.core
  (:require [overseer.api :as overseer])
  (:gen-class))

(def job-graph
  {:start []})

(def job-handlers
  {:start (fn [job] (println "Hello World!"))})

(defn -main [& args]
  (let [config {:datomic {:uri "datomic:free://localhost:4334/myapp"}}]
    (overseer/start config job-handlers)))
```

Once this is done, make sure that your main namespace is set to be AOT-compiled in your `project.clj` file. Then you're ready to build your code and start the worker!

```
lein uberjar
java -jar target/myapp-0.1.0-SNAPSHOT-standalone.jar myapp.core
```

You should see a series of "No handleable jobs found..." log messages, until you insert some jobs to run. Workers log basic information to stdout (via [Timbre](https://github.com/ptaoussanis/timbre)), and it can be useful to aggregate this information in a central log processor.

It's highly recommended to run Overseer within an external process supervisor such as [Upstart](http://upstart.ubuntu.com/). While ordinary exceptions within your handlers will not crash the system, it's still recommended to use a restarting supervisor for maximum uptime in case of an unexpected system error. An example Upstart configuration is provided [here](https://github.com/framed-data/overseer/blob/master/examples/upstart/overseer.conf).
