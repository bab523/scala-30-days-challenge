# Day 16 - Aggregations

## Topics covered

- Practice count, sum, avg, min and max
- Use groupBy with multiple columns
- Use HAVING-like filtering after aggregation
- Calculate department-wise salary statistics
- Scenario: generate hospital department revenue metrics

## Key notes

- df.agg(...) aggregates the whole DataFrame, groupBy(...).agg(...) aggregates per group.
- groupBy accepts multiple columns, for example groupBy("dept", "gender").
- Spark has no HAVING method on DataFrames. Filter after agg (filter on the aggregated column) or use HAVING in spark.sql.
- WHERE filters rows before grouping, HAVING filters groups after grouping.
- countDistinct counts unique values, stddev measures how spread out salaries are.
- pivot turns the values of a column (month) into separate columns.
- round(..., 2) keeps averages readable.

## Result

- Company: 12 employees, total salary 830000, average 69166.67.
- Highest average salary: Engineering (108333.33).
- Hospital total revenue: 8725000.
- Top department: Cardiology (3610000).
- Departments above 1,000,000 revenue: Cardiology, Orthopedics, Emergency.
- Orthopedics has the highest revenue per patient (19475.52).

## Data

- data/employees.csv (12 employees)
- data/hospital_billing.csv (15 bills)

## How to run

    cd day16
    sbt run
