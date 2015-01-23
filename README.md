# Overseer

Overseer is a Clojure framework for defining and running complex job pipelines. It allows one to define complex workflows as a graph of dependent jobs, and automatically handles scheduling, execution, and failure handling among a pool of workers.

## Installation

Conformity is available on Clojars, and can be included in your leiningen `project.clj` by adding the following to `:dependencies`:

```clj
[io.framed/overseer "0.1.0"]
```

Overseer stores its operarational data in Datomic and requires its schema to be installed into the database. This can be done at the REPL (you should only have to do this once):

```clj
(require '[datomic.api :as d])
(require 'overseer.schema)
(def uri "datomic:free://localhost:4334/overseer") ; Substitute your own as necessary
(overseer.schema/install (d/connect uri))
```

This should return `:ok` after a successful installation.

## Using Datomic Pro
By default, Overseer is configured to use the free distribution of Datomic, which uses transactor-local storage and is limited to 2 simultaneous peers. It's strongly suggested that Overseer be used in conjunction with [Datomic Pro](http://www.datomic.com/pricing.html) which supports more robust storage services such as DynamoDB/SQL and high availability. If you're already a user of Datomic Pro, you can use your own license with Overseer by putting the following in the `:dependencies` section of your `project.clj`:

```clj
[io.framed/overseer "0.1.0" :exclusions [com.datomic/datomic-free]]
```

## Using Overseer
You can find detailed information on defining and running Overseer jobs in the [wiki](https://github.com/framed-data/overseer/wiki).

## License

MIT (see [LICENSE](https://github.com/framed-data/overseer/blob/master/LICENSE))
