# Monitoring

At time of writing, Overseer does not include any sort of web UI for monitoring currently-running jobs or the queue of unstarted jobs. However, there are public functions available for querying the state of the system in `overseer.status`:

- `(overseer.status/jobs-unstarted db)` - Find all job IDs that are not yet started.

- `(overseer.status/jobs-with-status db status)` - Find all job IDs with a given status (e.g. `:unstarted`)

Since Overseer jobs are just entities in your database, you can query them however you like with your existing tools and systems. See `overseer.schema` for a reference of what Overseer stores.
