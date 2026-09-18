# Day 10 — Partitioning

## Tasks
- Inspect partition counts.
- Practice repartition and coalesce.
- Explain when increasing/decreasing partitions helps.
- Use partitionBy on a Pair RDD.
- Scenario: Optimize a dataset suffering from too few partitions.

## What I learned
- Default partition count depends on the source (file blocks) or `sc.parallelize`'s second argument.
- `repartition(n)` always triggers a full shuffle and can increase or decrease partitions.
- `coalesce(n)` avoids a full shuffle by merging partitions, but can only DECREASE partitions (not increase without forcing a shuffle).
- Increase partitions for more parallelism/load balance; decrease partitions to reduce scheduling overhead or produce fewer output files.
- `partitionBy(new HashPartitioner(n))` on a Pair RDD groups all records with the same key into the same partition — useful before joins/aggregations.
- Fixed a "too few partitions" scenario: a 1000-element RDD stuck on 1 partition (no parallelism) was fixed with `repartition(4)`, spreading work evenly (250 elements per partition) across 4 cores.

## How to run
```bash
sbt run
```
