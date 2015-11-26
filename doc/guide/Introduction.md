# What is Overseer?

Overseer is a library for building and running data pipelines in Clojure. It allows one to specify a graph (DAG) of tasks and dependencies between them, and can be used to build workflows for ETL, data ingestion, building complex reports, and more. It embeds into your application as a library, and is extremely simple to set up and use.

Conceptually, Overseer is in the same space as [Azkaban](https://github.com/azkaban/azkaban), [Airflow](https://github.com/airbnb/airflow), and [Luigi](https://github.com/spotify/luigi). However its written in Clojure for Clojure, and is not tied to the Hadoop ecosystem - Overseer simply lets you run arbitrary functions. From a design standpoint, Overseer favors ordinary Clojure data structures and functions over all else - no special classes or `InputFormats` are necessary here. This keeps the system and user code very simple, and makes things easy to test and reason about.
