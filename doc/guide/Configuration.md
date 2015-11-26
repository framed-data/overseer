# Configuration

Data pipelines are complex business, and Overseer includes a number of knobs for you to twiddle the system to your liking. However, it is designed to include sane defaults out of the box, and so all the available options are enumerated here for reference and when desired, but not all strictly necessary up front.

Overseer configuration is specified as a map of the following structure when calling `overeer.api/start`. Currently only the Datomic URI is required, and the rest are shown with default values.

```clj
{; Required
 :datomic {:uri "datomic:free://localhost:4334/myapp"}

 ; Optional: How long to sleep (in ms) if job queue is empty before checking again
 ; Default: 10000
 :sleep-time 10000

 ; Optional: Enable job heartbeats, whereby each node will periodically
 ; persist a timestamp via the DB and also act as a monitor that resets
 ; other jobs detected to be failing heartbeat checks
 ;
 ;   enabled - Default: true
 ;   sleep-time: How long to sleep between persisting heartbeats (per-worker)
 ;               Default: 60000
 ;   tolerance: How many heartbeats can fail before job is considered dead
 ;              to be reset by a monitor (Default: 5)
 :heartbeat
 {:enabled true
  :sleep-time 60000
  :tolerance 5}

 ; Optional: If present, errors in handlers will be logged to Sentry
 :sentry {:dsn "https://username:password@app.getsentry.com/port"}}
```
