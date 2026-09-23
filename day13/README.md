# Day 13 - Spark SQL Basics

## Topics covered

- Create a DataFrame from CSV and JSON data
- Inspect schema, select and filter columns
- Use withColumn and expressions
- Register a temporary view and run SQL
- Scenario: customer analytics report using Spark SQL

## Key notes

- spark.read.csv needs option header and inferSchema to get column names and types.
- spark.read.json expects one JSON object per line by default.
- printSchema shows the column names and types Spark inferred.
- withColumn adds or replaces a column, when/otherwise works like CASE WHEN.
- createOrReplaceTempView lets you query a DataFrame with plain SQL.
- LEFT JOIN keeps customers who have no orders, COALESCE turns their NULL totals into 0.

## Data

- data/customers.csv (5 customers)
- data/orders.json (8 orders)

## Result

- Hyderabad has the highest revenue (46369.5).
- Sara (C005) has no orders, so she shows up only because of the LEFT JOIN.

## How to run

    cd day13
    sbt run
