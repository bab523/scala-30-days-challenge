# Day 19 - Broadcast Join

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
- Driver has little memory, because the small table is first collected at the driver

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