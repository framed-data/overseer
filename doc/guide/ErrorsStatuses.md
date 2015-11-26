# Job errors and statuses

Overseer tracks a status for each job in the system. When you insert a job, it will be marked as `:unstarted` by default. When a worker claims a job, it automatically marks it as `:started`, runs the corresponding handler function, and finally marks the job `:finished`.

If an unexpected exception occurs in your handler function, the Overseer worker will *not* crash. Instead, Overseer catches the exception and automatically marks the job as `:failed`. In addition, Overseer has built-in support for [Sentry](https://getsentry.com/welcome/), so you can configure exceptions to automatically be sent to Sentry by adding the following to your configuration when starting the worker:

```clj
{:sentry
 {:dsn "https://username:password@app.getsentry.com/port"}}
```

There are a couple of helpers available for communicating status information to Overseer. Any any point in your code if you encounter a known error condition or invariant violation for example, you can call `(overseer.api/abort "error-message")`, which will *immediately* halt handler execution, log to stderr, send an error notice to Sentry, and finally mark the job status as `:aborted` and cancel scheduled execution of any dependent children jobs. Similarly, calling `(overseer.api/abort-silent "error-message")` has the same effect but does not send an error into Sentry.

There is another API helper available that can be used if you encounter a known transient failure and would like to try the job again at a later time (such as a network failure or issue with an external service). Calling `(overseer.api/fault "error-message")` will halt the current execution and mark the job as unstarted, to be picked up and run at a later time.
