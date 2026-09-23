# Day 15 - UDF Practice

## Topics covered

- Create a Scala UDF to classify salary bands
- Add calculated columns with withColumn
- Compare a UDF with a built-in Spark function
- Register a UDF with the Spark session/catalog
- Scenario: customer risk category from transaction values

## Key notes

- udf(...) wraps a Scala function so it can be used on DataFrame columns.
- spark.udf.register("name", func) makes the UDF usable inside spark.sql and shows up in spark.catalog.
- A UDF is a black box for Catalyst: no optimization or code generation inside it.
- The same logic with when/otherwise is a CASE WHEN that Catalyst can optimize, so it is usually faster.
- Rule of thumb: use built-in functions first, write a UDF only when no built-in can do the job.
- UDFs need care with null values, so handle nulls inside the function or filter them out first.

## Salary band rules

- HIGH: salary >= 100000
- MEDIUM: salary >= 50000
- LOW: below 50000

## Risk category rules

- HIGH: total >= 50000 or any single transaction >= 40000
- MEDIUM: total >= 5000, or any single transaction >= 1000, or 10+ transactions
- LOW: everything else

## Result

- HIGH risk: C004, C005
- MEDIUM risk: C002, C006
- LOW risk: C001, C003, C007

## How to run

    cd day15
    sbt run
