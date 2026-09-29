# Day 18 - Joins
- Inner, left, right and full joins
- Ambiguous column names fixed with aliases (c, o, p) and renaming
- Null handling after left join: isNull, coalesce, na.fill
- Scenario: orders + customers + payments

## Shuffle Sort Merge Join
Default join for two large tables in Spark, done in 3 steps:
1. Shuffle: both tables are repartitioned by the join key, so matching keys land in the same partition.
2. Sort: each partition is sorted by the join key.
3. Merge: the two sorted sides are scanned together and matching rows are joined.

It scales well because it does not need to hold a whole table in memory, but the shuffle (network + disk) is expensive.
If one table is small, a broadcast join avoids the shuffle.
