# Day 9 — Pair RDD

## Tasks
- Create key-value RDDs.
- Use reduceByKey, groupByKey and mapValues.
- Calculate revenue by product and department.
- Compare reduceByKey and groupByKey performance.
- Scenario: Aggregate bank transactions by account ID.

## What I learned
- A Pair RDD is an RDD of (key, value) tuples, created by mapping each record into a tuple like (product, revenue).
- `reduceByKey` combines values per key with a map-side pre-aggregation before shuffling, sending less data over the network.
- `groupByKey` shuffles all raw values first and groups them after, which uses more memory and network bandwidth.
- `mapValues` transforms only the value part of each pair, keeping the key unchanged (e.g. adding tax to revenue).
- For simple aggregations (sums, counts), reduceByKey is preferred over groupByKey for better performance.
- Applied this to a real scenario: summing bank transactions per account ID to get final balances and transaction counts.

## How to run
```bash
sbt run
```
