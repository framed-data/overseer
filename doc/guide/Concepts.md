# Concepts

There are a couple key concepts and terms that will help you in using Overseer. These will be elaborated upon in other sections.

- **Job Graph** - A Clojure map specifying abstract dependencies between different job types

- **Handler** - A Clojure function or map specifying code responsible for executing an instance of a job

- **Job** - A single concrete unit of work to execute. Jobs in Overseer are stored and managed in Datomic, and are ordinary Clojure maps with keyword keys.
   Jobs have types, which are simply keyword identifiers used in specifying dependencies and looking
   up corresponding handler functions. Overseer automatically tracks statuses or each job in the system as they are created and executed; for reference the set of valid statuses is `:unstarted :started :aborted :failed :finished`.

- **Worker** - A single instance of Overseer running on a machine is usually called a worker, which continually selects
  and executes eligible jobs from the central DB. Workers can be scaled out in a cluster of many machines.
  Contrary to what the project name may suggest, Overseer uses no master node and all workers are equal - the
  DB is the single point of coordination and workers never communicate directly with one another.
