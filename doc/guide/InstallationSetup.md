# Installation and Setup

Overseer is used as a library within your existing Clojure application. First you'll need to add it as a dependency in your `project.clj` file:

`[io.framed/overseer "0.7.2"]`

Overseer stores its operational data in [Datomic](http://www.datomic.com/), so you'll need to include that as a dependency as well. Here's an example `project.clj` file with all dependencies:

```clj
(defproject myapp "0.1.0-SNAPSHOT"
  :aot [myapp.core]
  :main myapp.core
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.datomic/datomic-free "0.9.5130"]
                 [io.framed/overseer "0.7.2"]])
```

By default, Overseer is configured to use the free distribution of Datomic, but Datomic Pro is supported as well (and highly recommended); just substitute your own Datomic Pro dependency instead of `datomic-free` as appropriate, and modify the Overseer dependency shown here with `[io.framed/overseer "0.7.2" :exclusions [com.datomic/datomic-free]]`. We'll be specifying a namespace to act as a main entry point and start the system later, so choose your main namespace and make sure its AOT-compiled.

Next up we'll set up Datomic and install Overseer's schema, so fire up `lein repl`. If you already have a Datomic database set up and running, you can substitute your URI here and Overseer will integrate with your existing DB, so you can skip this first step. Otherwise you'll need to make sure Datomic is running and create a database:

```clj
myapp.core=> (require '[datomic.api :as d])
myapp.core=> (def uri "datomic:free://localhost:4334/myapp")
myapp.core=> (d/create-database uri)
```

Next up we'll install Overseer's schema.

```
myapp.core=> (require '[overseer.schema])
myapp.core=> (overseer.schema/install (d/connect uri))
:ok
```

If everything went smoothly, you should see the `:ok` return value. At this point, Overseer is fully installed and ready to go!
