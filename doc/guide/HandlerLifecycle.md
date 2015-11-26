# The handler lifecycle

As mentioned previously, job handlers are plain Clojure functions that run on an instance of a job. This is not the complete truth however; it is often useful to separate an individual handler into stages, namely pre-processing, processing, and post-processing. This allows finer-grained control of input/output transformation, state management, and so on. To use these 'lifecycle stages', your job handler can *also* be a Clojure map of the following structure:

```clj
{:pre-process (fn [job] ...)
 :process (fn [job] ...)
 :post-process (fn [job result] ...)}
```

The `:pre-process` function is optional and runs before anything else, receiving the job map as an argument. It can be used to ex: set up prerequisite state for the main processor function.

The `:process` is required, and is the star of the show. It receives the job map as an argument. Here you'll typically load up input data and perform computations or transformations.

The `:post-process` function is optional and receives the job map as an argument as well as the return value of the `:process` step. Here one can perform final cleanup, transformations, or upload computed results.

For data tasks, you may find it useful to keep your `:process` functions as pure as possible - that is, load input data and compute whatever results are necessary, but return them as plain values and data structures instead of persisting or uploading them immediately. That way, the process stage is easily unit-testable, and the post-process stage can be made responsible for persisting/uploading results. You may even be able to share post-processors across handlers if you have a common format for uploading artifacts to S3 or saving into a database for example.

