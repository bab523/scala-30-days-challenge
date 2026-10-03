# Day 27 - Real-Time Banking Project

## Scenario
Process bank transactions in real time, aggregate per account, enrich with
branch risk data and detect suspicious bursts.

## Event Schema
TxnEvent(txnId, accountId, branchId, txnType, amount)
Input line: TXN_ID,ACCOUNT_ID,BRANCH_ID,TYPE,AMOUNT
Example: T1,ACC201,B01,DEBIT,2500

## Pipeline
Socket stream -> parse and validate -> aggregate by account (batch + running)
-> join with branch risk data -> alerts -> window burst detection

## Features
- Aggregation: reduceByKey per batch, updateStateByKey for running totals.
- Burst detection: reduceByKeyAndWindow (30s window, 10s slide). 5 or more
  transactions by one account in the window is flagged as suspicious.
- Reference join: branch_risk.csv (small file) loaded as an RDD and joined with
  leftOuterJoin, so transactions from unknown branches are flagged, not dropped.
- High risk rule: amount >= 50000 at a HIGH risk branch raises an alert.

## Cache / Persist and Partitioning
- Reference RDD: partitionBy(HashPartitioner(2)) + persist(MEMORY_ONLY).
  It is read once and reused in every batch instead of being re-read from disk.
- Each batch RDD is partitioned with the same partitioner before the join, so
  the big persisted reference side is not shuffled again.
- events and enriched streams are cached because several outputs use them.
  Without cache, parsing and joining would run again for each output.
- Reference data is small, so a broadcast variable would also work.

## How this application would run on YARN
1. Package: sbt package (jar is created in target/scala-*/).
2. Remove setMaster("local[2]") from the code and let spark-submit decide.
3. Submit:

```
spark-submit \
  --class Day27BankingStreaming \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 3 \
  --executor-cores 2 \
  --executor-memory 2g \
  --files data/reference/branch_risk.csv \
  target/scala-2.12/day27_2.12-0.1.jar
```

4. YARN flow:
   - The ResourceManager accepts the job and starts the ApplicationMaster in a container.
   - In cluster mode the driver runs inside the ApplicationMaster.
   - The ApplicationMaster asks for executor containers; NodeManagers start them.
   - The socket receiver occupies one core of an executor, so the number of
     cores must be greater than the number of receivers.
   - Each 5s batch becomes a normal Spark job; its tasks run on the executors.
5. Changes needed for production:
   - checkpoint directory must be on HDFS (e.g. hdfs:///checkpoints/day27) so it
     survives a driver restart.
   - Reference file on HDFS or shipped with --files.
   - localhost socket does not work across nodes. A real system would read
     from Kafka.
   - Logs: yarn logs -applicationId <id>. Status: yarn application -list.

## How to Run (local)
Terminal 1: nc -lk 9999
Terminal 2: sbt run

## Result
See output.txt for the run log.# Day 26 - Real-Time Healthcare Project

## Scenario
Monitor patient vital signs in real time, raise alerts for abnormal values
and detect patients with repeated abnormal readings.

## Event Schema
VitalEvent(patientId, heartRate, spo2, systolic, temperature)
Input line: PATIENT_ID,HEART_RATE,SPO2,SYSTOLIC_BP,TEMPERATURE
Example: P101,78,98,120,98.6

## Pipeline
Socket stream -> parse to VitalEvent -> drop invalid lines -> compare with
thresholds -> alerts -> running count (stateful) -> window check (repeated abnormal)

## Normal Ranges (Broadcast Variable)
| Vital | Min | Max |
|---|---|---|
| Heart rate | 50 | 120 |
| SpO2 | 92 | 100 |
| Systolic BP | 90 | 140 |
| Temperature | 95.0 | 100.4 |

Broadcast sends the read-only thresholds to each executor once, instead of
shipping them with every task.

## Accumulators
- total_readings, invalid_readings, abnormal_readings
- Updated only inside foreachRDD actions so retries do not double count.
- Only the driver reads the value.

## Stateful Processing
updateStateByKey keeps the running abnormal count per patient across batches.
Needs a checkpoint directory.

## Window Processing
reduceByKeyAndWindow (window 30s, slide 10s) counts abnormal readings per patient.
A patient with 3 or more abnormal readings in the window is marked CRITICAL.
This catches repeated problems and ignores one-off sensor glitches.

## How to Run
Terminal 1: nc -lk 9999
Terminal 2: sbt run

## Result
See output.txt for the run log.# Day 25 - Window Operations

## Scenario
Detect a sudden increase in transactions during a time window.

## Concepts
- Batch interval: how often Spark creates a micro-batch (5 sec).
- Window size: how much past data one calculation covers (30 sec in demo, 10 min in the task).
- Sliding interval: how often the window is recomputed (10 sec in demo).
- Window size and sliding interval must be multiples of the batch interval.
- When slide < window size, windows overlap, so one record is counted in more than one window.

## Operations Used
- countByWindow: number of transactions in each window.
- reduceByKeyAndWindow: rolling sales total per store.
- reduceByWindow: rolling total sales of all stores.

## Spike Detection
Window count is compared with the previous window count.
ALERT is printed if count >= 8 and count >= 2 x previous window count.
For the 10-minute scenario, only change the intervals to Minutes(10) and Minutes(1).

## Notes
- Window operations need a checkpoint directory.
- reduceByKeyAndWindow also has a faster version with an inverse function
  (adds new batch, subtracts the batch that left the window), which avoids
  recomputing the whole window.

## Input Format
TXN_ID STORE AMOUNT  (e.g. T1 STORE_A 500)

## How to Run
Terminal 1: nc -lk 9999
Terminal 2: sbt run

## Result
See output.txt for the run log.# Day 24 - Stateless vs Stateful Streaming

## Scenario
Maintain running transaction counts per bank account.

## Stateless Processing
- Each batch is processed independently, nothing is remembered.
- Operations used: map, filter, reduceByKey.
- Result: transaction count per account for the current batch only.

## Stateful Processing
- Spark remembers a state per key across batches.
- Operation used: updateStateByKey (new values + old state = new state).
- Result: running total count and amount per account since the start.
- Needs a checkpoint directory so the state can be saved and recovered.

## Comparison
| | Stateless | Stateful |
|---|---|---|
| Memory of past batches | No | Yes |
| Example output | ACC101 count=1 | ACC101 total_count=3 |
| Checkpoint needed | No | Yes |
| Cost | Low | State grows with number of keys |

## Input Format
ACCOUNT TYPE AMOUNT  (e.g. ACC101 DEPOSIT 5000)
Lines with wrong format or invalid amount are filtered out.

## How to Run
Terminal 1: nc -lk 9999
Terminal 2: sbt run

## Result
See output.txt for the run log.# Day 23 - DStreams Basics

## Scenario
Stream application logs and count ERROR messages every interval.

## Concepts
- StreamingContext: entry point of Spark Streaming. Created with a batch interval (5 seconds here).
- socketTextStream: reads text lines from a TCP socket (localhost:9999), fed using netcat.
- DStream: a continuous sequence of RDDs, one RDD per batch interval.
- Transformations used:
  - map: trim each log line, extract log level
  - filter: keep only ERROR lines
  - flatMap: split lines into words
  - reduceByKey: count per log level / per word
- count(): number of ERROR lines in each batch.

## Micro-batch Processing
Spark Streaming does not process record by record. It collects the data
received during one batch interval (5 sec) into an RDD and runs normal Spark
jobs on it. So a stream is treated as a series of small batch jobs.
Latency is at least the batch interval.

## Notes
- master must be local[2] or more: one thread for the receiver, one for processing.
- Batches with no incoming data show count 0.

## How to Run
Terminal 1: nc -lk 9999
Terminal 2: sbt run

## Result
See output.txt for the run log.# Day 19 - Broadcast Join

## What I built
- Large fact DataFrame: 2 million transactions (txn_id, branch_id, amount)
- Small reference DataFrame: branch master (5 branches)
- Joined them with `broadcast(branches)` and compared with a sort merge join

## What is a broadcast join?
Spark sends a full copy of the small table to every executor. Each executor joins its
own partitions of the big table against that local copy. The big table is never shuffled.

## When is broadcast join appropriate?
- One side is small enough to fit in memory on every executor (default limit: 10 MB,
  set by `spark.sql.autoBroadcastJoinThreshold`, can be raised carefully)
- The other side is large
- Typical case: fact table + dimension / lookup / master table (branches, countries, products)
- Equi-joins and left joins where the small table is on the right side

## When NOT to use it
- Both tables are large (broadcasting a big table causes out-of-memory errors)
- The "small" table keeps growing over time
# Day 22 - Batch Mini Project (E-commerce Daily Sales Pipeline)

## Pipeline Flow
Raw CSV -> Remove duplicates -> Join (customers, products) -> Validate -> Calculate revenue -> Aggregate daily -> Partitioned Parquet

## Input Data (data/raw)
- transactions.csv: raw orders (includes bad records on purpose)
- customers.csv: customer details
- products.csv: product details and unit price

## Cleaning Rules
Records are rejected if:
- duplicate txn_id
- missing or unknown customer
- unknown product
- qty <= 0
- invalid date
- payment status is not PAID

## Key Decisions
- Left join used so unknown ids are caught instead of silently dropped.
- Rejected records saved separately in output/rejected with a reject_reason for audit.
- Revenue = qty * unit_price.
- Output written with partitionBy(year, month); repartition before write keeps files per folder low.

## Output
- output/daily_sales/year=YYYY/month=M/part-*.parquet
- output/rejected/part-*.csv

## How to Run
sbt run

## Result
See output.txt for the full run log.- Driver has little memory, because the small table is first collected at the driver

## Broadcast join vs shuffle sort merge join
| | Broadcast hash join | Shuffle sort merge join |
|---|---|---|
| Shuffle of big table | No | Yes (both sides) |
| Sort | No | Yes (both sides) |
| Memory need | Small table must fit on each executor | Low, can spill to disk |
| Best for | Big table + small table | Two big tables |
| Speed | Usually much faster | Slower because of shuffle and sort |
| Plan shows | BroadcastHashJoin | SortMergeJoin, Exchange, Sort |

## Scenario
Millions of transactions joined with a small branch master: broadcast the branch master,
so no transaction rows move across the network.
[error] java.lang.RuntimeException: no main class detected[0J
[0J[error] 	at scala.sys.package$.error(package.scala:28)[0J
[0J[error] stack trace is suppressed; run last Compile / run for the full output[0J
[0J[error] elapsed time: 2 s
[error] (Compile / run) no main class detected[0J
[0J[0J
