# Scaling out workers

Overseer is a masterless system, and workers do not communicate directly with one another - you can start arbitrarily many workers as part of a cluster coordinating via the central DB. Thus, scaling is limited only by the performance of the backing storage layer (Overseer is read-heavy, which scales well), as well as your Datomic license :-)

Note that Overseer workers do *not* parallelize job execution within a single machine - each Overseer process runs one job at a time on a single thread. The system is designed to allow handlers to maximize resource utilization of the system by making use of parallelism within their own code.
