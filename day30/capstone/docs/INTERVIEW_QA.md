# 20 Interview Questions from My Capstone Project

**1. Walk me through your project.**
E-commerce pipeline. Batch job: raw RDD, clean, enrich with DataFrame joins, aggregate with SQL and windows, write Parquet. Streaming job: new order files are processed in micro-batches into windowed revenue per category.

**2. Why RDD for cleaning but DataFrame for the rest?**
Row-level parsing with try/catch and accumulators is natural in RDD. Joins, windows and group-bys benefit from the Catalyst optimizer and Tungsten, so DataFrame is faster there.

**3. What is lineage?**
The graph of transformations that created an RDD. Spark replays it to rebuild lost partitions. I printed it with toDebugString.

**4. What is the DAG and who creates it?**
A graph of operations built by the driver when an action is called. The DAGScheduler splits it into stages at shuffle boundaries.

**5. Where are the shuffles in your job?**
reduceByKey on orderId, sort-merge join with customers, repartition by customerId, and the window partitioned by category. The broadcast join has no shuffle.

**6. Narrow vs wide transformation?**
Narrow (map, filter, flatMap): each output partition depends on one input partition, pipelined in one stage. Wide (reduceByKey, join, repartition): data moves across the network, which creates a new stage.

**7. Why broadcast the products table?**
It is tiny (12 rows). Sending it to every executor avoids shuffling the large orders data.

**8. When is broadcast a bad idea?**
When the table is large. It can cause driver or executor out-of-memory errors. Spark's auto limit is 10 MB by default.

**9. What is an accumulator and what is the catch?**
A write-only-for-executors, read-on-driver counter. I used it for bad and cancelled rows. In transformations, task retries or recomputation can double count. Only updates inside actions are guaranteed exactly once.

**10. Accumulator vs broadcast?**
Broadcast sends read-only data to executors. Accumulator collects counters from executors back to the driver.

**11. cache vs persist? Why MEMORY_AND_DISK?**
cache uses the default level, persist lets me choose. MEMORY_AND_DISK spills to disk instead of recomputing when memory is short.

**12. What did you persist and why?**
The cleaned RDD (used by count and toDF) and the enriched DataFrame (used by SQL, windows, UDF aggregation and writes). Without persist, the whole lineage would run again for each action.

**13. How did you tune partitions?**
shuffle.partitions set to 8 instead of 200 for small data, repartition by customerId before windows and groupBys, coalesce(1) before writing the small CSV.

**14. repartition vs coalesce?**
repartition does a full shuffle and can increase or decrease partitions. coalesce avoids a full shuffle and can only decrease.

**15. Why only one UDF?**
UDFs are black boxes to Catalyst, so no optimization and extra serialization. The tier rule is custom business logic, so a UDF was justified. Everything else uses built-in functions.

**16. How did you handle dirty data?**
The parser drops rows with wrong column count, missing orderId, non-numeric or non-positive price and qty. Duplicates are removed with reduceByKey on orderId. Cancelled orders are filtered. Counts are tracked with accumulators.

**17. How does your streaming job work?**
readStream on a folder with an explicit schema, one file per trigger, stream-static broadcast join with products, 30 second windows with a 1 minute watermark, output mode update to console, with a checkpoint.

**18. What is a watermark?**
It tells Spark how late data can arrive. Windows older than max event time minus 1 minute are finalized and their state is dropped, which keeps state memory bounded.

**19. How do you deploy this on YARN?**
Build the jar with sbt package, then spark-submit with master yarn, deploy-mode cluster, and executor count, cores and memory. The ApplicationMaster requests containers from the ResourceManager and executors run inside them. Input paths move to HDFS or S3.

**20. How would you make it production ready?**
Kafka source instead of files, Delta or Parquet sink with exactly-once semantics, enable AQE, unit tests for parse, monitoring through Spark UI and history server, and a scheduler such as Airflow for the batch job.
