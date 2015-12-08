# Specifying job graphs and handlers

Overseer uses Clojure data structures and functions to specify your tasks and the dependencies between them. There are two important concepts here - job graphs and job handlers. We'll first include the complete code to specify a job dependency graph and some handlers, and then walk through it.

```clj

(def job-graph
  {:start []
   :result1 [:start]
   :result2 [:start]
   :finish [:result1 :result2]})

(def job-handlers
  {:start (fn [job] (println "start"))
   :result1 (fn [job] (println "result1"))
   :result2 (fn [job] (println "result2"))
   :finish (fn [job] (println "finish"))})
```

## Job Graphs

There are a few important components at play here. First is `job-graph`: this is an ordinary Clojure map that abstractly describes your jobs and the dependencies between them; you'll see that job types are specified as keywords. Each job describes the jobs it relies on, i.e. its "parents" (this may be somewhat the reverse of other graph notations where each node describes its children, i.e. all arrows going downwards). Here we can see that the `:start` job has no dependencies, and so is eligible for execution right away. The `:result1` and `:result2` jobs both depend on `:start`, and they will not run until the `:start` job successfully completes. Similarly, the `:finish` job depends on both `:result1` and `:result2`. Overseer handles scheduling and execution for you, so if any job fails unexpectedly, its children will not run.

Overseer does some magic behind the scenes - since `:result1` and `:result2` do not depend on each other, as soon as the `:start` job finishes, both `:result1` and `:result2` may start executing immediately in parallel on different machines in your cluster, depending on your configuration!

## Job Handlers

The next important concept is `job-handlers`. This is a Clojure map where the keys are job types corresponding to the dependency graph from before, and the values are ordinary Clojure functions to run. Overseer will automatically call these functions and pass in a `job` argument, which is a map of information about the current job, a single concrete unit of work being executed. The example functions just print a simple message to stdout and don't do anything meaningful; real jobs of course will likely perform computation and persist their results to external storage, enabling data dependencies between jobs.

**Important!**

It is strongly recommended that your job handlers be *idempotent*. That is, Overseer does *not* explicitly attempt to provide "exactly-once" execution guarantees for any instance of a job. Especially if you use certain helpers such as `overseer.api/fault`, multiple attempts at executing a job are entirely possible and in certain cases desirable. A common pattern is to operate only on known inputs/arguments, compute a result, and set or upload a deterministic key in a key-value store for example, rather than dangerously having an effect on shared mutable state, internal or external.
