# Day 7 — Immutability, Lineage and Fault Tolerance

## Tasks
- Create a multi-step RDD transformation chain.
- Draw its lineage.
- Explain why RDDs are immutable.
- Explain how Spark recomputes lost partitions.
- Scenario: Simulate executor loss conceptually and identify what Spark recomputes.

## What I learned
- A multi-step chain (textFile -> map -> filter -> map -> filter) builds a DAG, not actual data, until an action runs.
- `rdd.toDebugString` prints the lineage — the chain of parent RDDs and transformations used to build the final RDD.
- RDDs are immutable so Spark can always recompute lost data from lineage instead of tracking in-place mutations.
- If an executor is lost, Spark's DAGScheduler recomputes only the missing partition(s) by re-running the lineage from the nearest available source (original file or a cached ancestor).
- Caching an intermediate RDD (`.cache()`) can save recomputation time by giving Spark a closer starting point.

## How to run
```bash
sbt run
```
