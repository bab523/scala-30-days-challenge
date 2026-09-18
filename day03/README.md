cd ~/scala-30-days-challenge
cat > day3/README.md << 'EOF'
# Day 3 — Spark Setup and First Application

## Tasks
- Create a Scala Spark project with sbt.
- Create a SparkSession and SparkContext.
- Read a text file and display its contents.
- Explain driver, executor and cluster manager.
- Scenario: Run the same application in local mode with 2 and 4 cores.

## What I learned
- `SparkSession` is the entry point to Spark SQL/DataFrame APIs; `SparkContext` is the entry point to low-level RDD APIs.
- `sc.textFile()` reads a text file into an RDD, where each line becomes an element.
- Driver: runs the main() function and coordinates the application.
- Executor: worker processes that actually run tasks and store data.
- Cluster Manager: allocates resources (local, standalone, YARN, Kubernetes). We used `local[2]` mode.
- Faced a Java 17 module-access issue with Spark; fixed it by adding `--add-opens` JVM options in build.sbt.

## How to run
```bash
sbt run
```
EOF
