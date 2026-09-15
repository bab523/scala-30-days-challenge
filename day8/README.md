# Day 8 — DAG and Spark Execution

## Tasks
- Create a job with several transformations and actions.
- Identify stages and shuffle boundaries.
- Explain jobs, stages, tasks and partitions.
- Compare narrow and wide transformations.
- Scenario: Predict the number of stages for a reduceByKey pipeline.

## What I learned
- A **Job** is triggered by an action (collect, count). Multiple actions = multiple jobs.
- A **Stage** is a group of narrow transformations that run without a shuffle; a new stage starts at every wide transformation.
- A **Task** is the smallest unit of work — one task per partition within a stage.
- **Narrow transformations** (map, filter, flatMap, union) don't need data movement across partitions.
- **Wide transformations** (reduceByKey, groupByKey, sortByKey, join, distinct) require a shuffle and create a new stage boundary.
- For a pipeline like `textFile -> map -> filter -> reduceByKey -> map -> collect()`, the shuffle at `reduceByKey` splits it into exactly 2 stages.

## How to run
```bash
sbt run
```
