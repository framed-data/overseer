# Overseer

Overseer is a library for building and running data pipelines in Clojure.
It allows for defining workflows as a graph (DAG) of dependent tasks, and handles
scheduling, execution, and failure handling among a pool of workers.

## Installation

Overseer is available on Clojars, and can be included in your leiningen `project.clj` by adding the following to `:dependencies`:

[![Clojars Project](http://clojars.org/io.framed/overseer/latest-version.svg)](http://clojars.org/io.framed/overseer)

## Usage
If you're looking to get up and running in just a few minutes, check out the [quickstart](https://github.com/framed-data/overseer/wiki/Quickstart).
The [User Guide](https://www.gitbook.com/book/framed/overseer/) contains comprehensive information on installing, configuring, and using Overseer. Finally, you can find nitty-gritty details in the [API docs](https://framed-data.github.io/overseer).

## License
Eclipse Public License v1.0 (see [LICENSE](https://github.com/framed-data/overseer/blob/master/LICENSE))
