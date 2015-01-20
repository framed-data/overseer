# Overseer

TODO

## Installation

Overseer stores its operarational data in Datomic and requires its schema to be installed into the database. This can be done at the REPL (you should only have to do this once):

```clj
(require 'overseer.schema)
(def conn (connect-to-my-datomic)) ; This step is up to you
(overseer.schema/install conn)
```

This should return `:ok` after a successful installation.


## Using Datomic Pro
By default, Overseer is configured to use the free distribution of Datomic, which uses transactor-local storage and is limited to 2 simultaneous peers. It's strongly suggested that Overseer be used in conjunction with [Datomic Pro](http://www.datomic.com/pricing.html) which supports more robust storage services such as DynamoDB/SQL and High Availability.

TODO: integrating with Datomic pro


## Job handlers and the job graph
TODO


## Running jobs
TODO - how to construct a txn, how to specify additional args
(core/->graph-txn graph tx)



## Error handling in jobs
You are welcome to do your own error/exception handling within your job handlers however you please. By default, Overseer will catch exceptions within handlers and log an error. Overseer also supports sending handler exceptions directly to [Sentry](https://getsentry.com/) by using the following options in your config file:

```yaml
sentry:
  dsn: https://public_key:secret_key@app.getsentry.com/port
```

## Starting a worker
TODO

## Scaling out workers

Since Overseer uses the database as the central point of truth, the number of workers in the system can be arbitrarily scaled out horizontally, limited only by the number of Datomic peers in your license :-). As such, Overseer does not actually utilize any 'master' or 'supervisor' process.


## License

MIT (see [LICENSE](https://github.com/framed-data/overseer/blob/master/LICENSE))
