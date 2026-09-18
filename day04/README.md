
## Tasks
- Create RDDs from collections and text files.
- Use map, filter and flatMap on an RDD.
- Calculate total sales from transaction records.
- Inspect partitions and explain default parallelism.
- Scenario: Process a large customer file split into multiple partitions.

## What I learned
- RDDs can be created from a Scala collection (`sc.parallelize`) or from a file (`sc.textFile`).
- `map`, `filter`, and `flatMap` transform RDDs lazily until an action (like `collect`) is called.
- `reduceByKey` aggregates values by key (useful for grouping sales by customer).
- Default parallelism usually matches the number of cores given to Spark (`local[2]` → 2).
- `repartition(n)` changes the number of partitions, useful for scaling processing across a cluster.
- Hit a Kryo serialization issue on Java 17; fixed it with extra `--add-opens` JVM flags in build.sbt.
