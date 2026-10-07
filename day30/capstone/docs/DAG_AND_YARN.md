# DAG, Lineage, Stages, Executors and YARN

## Lineage
Lineage is the chain of transformations that built an RDD (see `clean.toDebugString` in docs/batch_output.txt).
If a partition is lost, Spark recomputes only that partition from its parents. That is how Spark is fault tolerant without replicating data.

## DAG
Transformations are lazy. An action (`count`, `show`, `write`) makes the driver build a DAG of operations.
The DAGScheduler cuts the DAG at every shuffle (wide dependency) into stages.

## Stages and tasks in this project
- Stage 1: textFile, filter, flatMap(parse), map. These are narrow, so they are pipelined in one stage.
- Stage 2: after reduceByKey (shuffle), dedupe, filter cancelled, persist.
- Later stages: sort-merge join with customers, repartition by customerId, window by category.
- One task = one partition. Tasks of a stage run in parallel on executors.

## Executors and driver
- Driver: runs main(), builds the DAG, schedules tasks, collects accumulator values.
- Executor: JVM process on a worker node. Runs tasks, caches persisted data, writes shuffle files.
- Total parallelism = number of executors x cores per executor.
- Local mode here: driver and executors live in one JVM (master local[*]).

## Broadcast vs accumulator
- Broadcast: read-only value sent once to every executor (products table).
- Accumulator: executors only add, the driver reads (badRows, cancelledRows).
  Updates inside transformations can double count if a task is retried. Reliable only inside actions.

## YARN deployment
YARN components: ResourceManager (cluster scheduler), NodeManager (per node), ApplicationMaster (one per app, asks for containers).
Each Spark executor runs inside a YARN container.

Package the jar:

    sbt package

Submit in cluster mode (driver runs inside the cluster):

    spark-submit \
      --class CapstoneBatch \
      --master yarn \
      --deploy-mode cluster \
      --num-executors 4 \
      --executor-cores 2 \
      --executor-memory 4g \
      --driver-memory 2g \
      --conf spark.sql.shuffle.partitions=200 \
      --conf spark.dynamicAllocation.enabled=true \
      target/scala-2.12/capstone_2.12-1.0.jar

- client mode: driver runs on the submitting machine (good for debugging, spark-shell).
- cluster mode: driver runs in the ApplicationMaster container (good for production, survives client disconnect).
- Input paths must change from local data/ to hdfs:// or s3a:// paths.
- Check logs with: yarn logs -applicationId <app_id>
- Spark master must be set by spark-submit, so remove .master("local[*]") from the code before deploying.

## Cache vs persist
cache() = persist(MEMORY_ONLY) for RDDs, MEMORY_AND_DISK for DataFrames.
persist(level) lets you choose the storage level. Use unpersist() when done.

## Partition tuning
- repartition(n, col): full shuffle, balances data and co-locates keys for later joins and windows.
- coalesce(n): no full shuffle, only reduces partitions (used before writing CSV).
- spark.sql.shuffle.partitions: 8 locally. Default 200 is for large data.
- Rule of thumb: 2 to 4 partitions per core, about 128 MB per partition.
