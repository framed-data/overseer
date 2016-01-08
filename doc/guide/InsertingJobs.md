# Inserting jobs

Once your worker is up and running, you'll need to insert some jobs into the system to get any work done. Jobs are entities tracked in Datomic, and inserting jobs is just asserting some transactions. Overseer provides helpers to construct such transactions.

## Inserting an entire graph

Given a sample graph like:

```clj
(def job-graph
  {:start []
   :result1 [:start]
   :result2 [:start]
   :finish [:result1 :result2]})
```

You can insert the entire graph at once, i.e. create unstarted job instances for `:start`, `:result1`, `:result2`, and `:finish`, and have them be executed by any available workers as their dependencies become satisfied. `overseer.api/graph-txns` builds the sequence of transactions for a graph.

```
myapp.core=> (require '[overseer.api :as overseer])
myapp.core=> (require '[myapp.core :as myapp])
myapp.core=> (def txns (overseer/graph-txns myapp/job-graph))
myapp.core=> @(d/transact (d/connect uri) txns)
```

You can also specify arguments to a job, which will be attached as attributes of the entity. As such, you'll need to make sure that any attributes already exist in your Datomic schema. `graph-txns` optionally accepts a second argument which is a map of attributes for your job:


```clj
myapp.core=> (def txns (overseer/graph-txns myapp/job-graph {:organization-id 123 :user-id 456}))
myapp.core=> @(d/transact (d/connect uri) txns)
```

Note that these arguments will be attached to *every* job that gets created; it's assumed a job graph will be operating for a single shared context such as a user, organization, or other meaningful value in your domain.

You can also construct a transaction to insert a single job. `overseer.api/job-txn` accepts a keyword job type, and optionally arguments to attach to the job entity.

```clj
(def txn
  (overseer.api/job-txn :my-job-type {:organization-id 123 :user-id 456}))

@(d/transact conn [txn])
```

**Important**

Overseer explicitly does *not* provide execution ordering guarantees. If you assert two jobs or job graphs in order, there is no guarantee that the jobs will be executed in that same order. All jobs are executed only once their dependencies are completed, but executed jobs are selected randomly from the pool of all eligible jobs.
