# Day 25 - Window Operations

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
