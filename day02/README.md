# Day 2 — Scala Collections Practice

## Tasks
- Process a sales List using map, filter, flatMap and reduce.
- Use Vector for indexed customer records and explain why it is useful.
- Use Map to calculate product quantities and prices.
- Write a for-comprehension combining customers and orders.
- Scenario: Produce a daily sales summary without Spark.

## What I learned
- `map` transforms each element, `filter` selects elements, `flatMap` flattens nested collections, `reduce` combines all elements into one value.
- `Vector` gives faster random/indexed access compared to `List`.
- `Map` is useful for quick key-based lookups (like product -> price).
- `for-yield` can join two collections together (similar to a SQL join).
- `groupBy` is handy for aggregating data (like product-wise revenue).

## How to run
```bash
scala Main.scala
```
