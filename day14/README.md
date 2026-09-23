# Day 14 - DataFrame and Dataset

## Topics covered

- Create a case class and convert DataFrame to Dataset
- Convert Dataset back to DataFrame
- Compare RDD, DataFrame and Dataset
- Explain type safety and Catalyst optimization
- Scenario: typed employee payroll pipeline

## Key notes

- df.as[Employee] turns a DataFrame into a Dataset[Employee].
- ds.toDF() turns it back. A DataFrame is just Dataset[Row].
- Case classes must be defined outside the object so Spark can create encoders.
- Type safety: a wrong column name in a DataFrame fails at runtime, a wrong field name in a Dataset fails at compile time.
- Catalyst optimizes column expressions (like col("salary") > 60000) and can push filters to the data source. Lambdas (e => e.salary > 60000) are opaque to Catalyst.

## RDD vs DataFrame vs Dataset

| Feature | RDD | DataFrame | Dataset |
|---|---|---|---|
| Schema | No | Yes | Yes |
| Type safety | Compile time | Runtime | Compile time |
| Catalyst optimizer | No | Yes | Yes |
| Best for | Low level control | SQL style analytics | Typed business logic |

## Scenario

Employees are read from CSV as Dataset[Employee], mapped to Dataset[Payroll] with bonus, tax slab and net pay, then summarized by department.

## How to run

    cd day14
    sbt run
