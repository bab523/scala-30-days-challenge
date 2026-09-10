
## Tasks
- Practice map, filter, flatMap, distinct and union.
- Practice count, collect, first, take and reduce.
- Explain transformations versus actions.
- Identify which operations are lazy.
- Scenario: Build a log analyzer that counts ERROR messages.

## What I learned
- Transformations (map, filter, flatMap, distinct, union, reduceByKey) are LAZY — they just build a logical plan (DAG).
- Actions (count, collect, first, take, reduce, foreach) trigger actual computation.
- Laziness lets Spark optimize the execution plan before running anything.
- `distinct()` removes duplicates after a `union()`, useful for combining two datasets cleanly.
- Built a simple log analyzer: filtered ERROR lines and counted them using `filter` + `count`, and grouped log levels using `map` + `reduceByKey`.
