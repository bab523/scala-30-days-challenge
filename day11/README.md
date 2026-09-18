# Day 11 — Broadcast and Accumulators

## Tasks
- Broadcast a small product reference map.
- Use an accumulator to count bad records.
- Explain why normal driver variables should not be used for distributed updates.
- Combine broadcast data with RDD processing.
- Scenario: Validate transactions against a small master table.

## What I learned
- `sc.broadcast()` sends a read-only copy of small reference data to every executor ONCE, instead of shipping it with every task — saves network bandwidth.
- `sc.longAccumulator()` lets executors safely report counts (like bad records) back to the driver, something a plain Scala variable cannot do because each executor would get its own isolated copy.
- Normal driver variables don't work for distributed counting because closures are serialized per-executor; updates made inside a task never propagate back to the driver.
- Combined broadcast lookup with RDD `map` to validate each transaction's product ID against a master table.
- Bonus gotcha: calling `.collect()` multiple times on a non-cached RDD re-executes the whole pipeline each time, so an accumulator can get updated more than expected. Using `.cache()` after defining the RDD avoids this.

## How to run
```bash
sbt run
```
