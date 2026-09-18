import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day8-DAG-App")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ---------- Task 1: Job with several transformations and actions ----------
    println("---- Task 1: Job with transformations and actions ----")

    val ordersRdd = sc.textFile("orders.txt")                       // transformation
    val parsedRdd = ordersRdd.map(line => {                          // transformation (narrow)
      val parts = line.split(",")
      (parts(0), parts(2).toDouble)
    })
    val filteredRdd = parsedRdd.filter(_._2 > 1000)                  // transformation (narrow)
    val totalsRdd = filteredRdd.reduceByKey(_ + _)                   // transformation (wide - shuffle)
    val sortedRdd = totalsRdd.sortByKey()                            // transformation (wide - shuffle)

    val result = sortedRdd.collect()                                 // ACTION 1 (triggers job)
    println("Customer totals (orders > 1000):")
    result.foreach { case (cust, total) => println(s"  $cust -> $total") }

    val count = sortedRdd.count()                                    // ACTION 2 (triggers another job)
    println(s"Total customers with big orders: $count")

    // ---------- Task 2: Identify stages and shuffle boundaries ----------
    println("\n---- Task 2: Stages and shuffle boundaries ----")
    println(sortedRdd.toDebugString)
    println("\nExplanation:")
    println("Stage 1: textFile -> map -> filter (all narrow transformations, no shuffle)")
    println("SHUFFLE BOUNDARY occurs at reduceByKey (data must be grouped by key across partitions)")
    println("Stage 2: reduceByKey -> sortByKey (sortByKey also needs a shuffle to order globally)")
    println("So this job has 2 stages, split at the reduceByKey shuffle boundary.")

    // ---------- Task 3: Explain jobs, stages, tasks and partitions ----------
    println("\n---- Task 3: Jobs, Stages, Tasks, Partitions ----")
    println("Job: triggered by an ACTION (like collect() or count()). We called 2 actions,")
    println("     so Spark created 2 separate jobs for this program.")
    println("Stage: a group of transformations that can run WITHOUT a shuffle. A new stage")
    println("       starts whenever a wide transformation (like reduceByKey) requires data")
    println("       to move between partitions.")
    println("Task: the smallest unit of work - one task runs on ONE partition of data within")
    println("      a stage. If a stage's RDD has 4 partitions, Spark launches 4 tasks.")
    println("Partition: a chunk of the dataset. More partitions = more parallelism, but also")
    println("           more overhead if too small.")
    println(s"Number of partitions in ordersRdd: ${ordersRdd.getNumPartitions}")
    println(s"Number of partitions in sortedRdd: ${sortedRdd.getNumPartitions}")

    // ---------- Task 4: Narrow vs Wide transformations ----------
    println("\n---- Task 4: Narrow vs Wide transformations ----")
    println("Narrow transformation: each output partition depends on ONE input partition.")
    println("  Examples: map, filter, flatMap, union. No data movement across the network,")
    println("  so these can run within the same stage.")
    println("Wide transformation: output partitions depend on MULTIPLE input partitions,")
    println("  requiring a SHUFFLE (data movement across the cluster).")
    println("  Examples: reduceByKey, groupByKey, sortByKey, join, distinct.")
    println("  Wide transformations always create a new stage boundary.")

    // ---------- Task 5: Scenario - predict stages for a reduceByKey pipeline ----------
    println("\n---- Task 5: Predicting stages for a reduceByKey pipeline ----")
    println("Given pipeline: textFile -> map -> filter -> reduceByKey -> map -> collect()")
    println("Step-by-step stage prediction:")
    println("  textFile   -> narrow (read source)")
    println("  map        -> narrow (same stage)")
    println("  filter     -> narrow (same stage)          }  STAGE 1")
    println("  reduceByKey -> WIDE (shuffle happens here)  <- stage boundary")
    println("  map        -> narrow (after shuffle)")
    println("  collect()  -> action (same stage)           }  STAGE 2")
    println("Prediction: this pipeline will have exactly 2 STAGES, split at the single")
    println("reduceByKey shuffle. Only ONE action (collect) means only ONE job is triggered.")

    spark.stop()
  }
}
