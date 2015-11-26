# Advanced - Harnesses


Harnesses are a mechanism to 'wrap' your job handlers, giving you the ability to provide additional context, inject dependencies, or otherwise alter the normal flow of execution. Conceptually, harnesses are similar to the idea behind Ring middleware.

`overseer.api/harness` accepts a standard job handler (map or function) and a `wrapper` function which will be called with the *function* specified in your handler and is expected to return a new *function* with the same signature. If your handler is a map, it will be transparently constructed/deconstructed; harnesses work solely in terms of functions.

For example, a harness that does nothing and simply calls the original handler (i.e., the default behavior) is:

```clj
(defn my-harness [f]
  (fn [job]
    (f job)))
```

A more substantive harness can be used to provide jobs with additional
context or inject dependencies such as a database connection:

```clj
(defn my-harness [f]
  (fn [job]
    (let [modified-job (assoc job :conn (d/connect my-datomic-uri))]
      (f modified-job))))
```

Or add logging:

```clj
(defn logging-harness [f]
  (fn [job]
    (println "START execution of " (:job/id job))
    (f job)
    (println "FINISH execution of " (:job/id job))))
```

If you'd like, you can even write your own exception harness and catch handler exceptions before they get to Overseer, for example to perform application-specific catching logic.

```clj
(defn exception-harness
  "Intercept handler exceptions and perform application-specific
   work before re-propagating exception for Overseer"
  [f]
  (fn [job]
    (try (f job)
      (catch Exception ex
        ; application-specific logic here ...
        (throw ex))))) ; Re-propagate to Overseer for default behavior (or not)
```

After defining a harness, in the `job-handlers` map one specifies

```clj
{:my-job (overseer.api/harness my-job/run my-harness)}
```

Following the previous example, within your handler you now have
additional context available:

```clj
(defn run [{:keys [conn] :as job}] ...)
```

If your handler is a map, you can optionally specify a key to harness a
specific stage; the default is `:process`. For example, to harness
a post-processor:

```clj
{:my-job (overseer.api/harness my-job/run :post-process my-harness)}
```

If you attempt to harness a missing stage for a given job, the wrapper will be invoked with a properly-formed identity function, meaning you can write your harnesses in a single consistent fashion, and, for example, universally harness a post-processor for a set of handlers that may or may not define their own post-processor.
