import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day7-Lineage-App")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ---------- Task 1: Multi-step RDD transformation chain ----------
    println("---- Task 1: Multi-step RDD transformation chain ----")

    val numbersRdd = sc.textFile("numbers.txt")               // Step 1: read file
    val intsRdd = numbersRdd.map(line => line.trim.toInt)      // Step 2: convert to Int
    val evenRdd = intsRdd.filter(n => n % 2 == 0)               // Step 3: keep even numbers
    val doubledRdd = evenRdd.map(n => n * 2)                    // Step 4: double them
    val resultRdd = doubledRdd.filter(n => n > 50)              // Step 5: keep > 50

    println(s"Final result: ${resultRdd.collect().mkString(", ")}")

    // ---------- Task 2: Draw its lineage ----------
    println("\n---- Task 2: Lineage (toDebugString) ----")
    println(resultRdd.toDebugString)
    println("\nLineage chain explanation:")
    println("numbers.txt -> textFile (base RDD)")
    println("  -> map (String to Int)")
    println("    -> filter (even numbers only)")
    println("      -> map (double the value)")
    println("        -> filter (keep values > 50)  [resultRdd]")

    // ---------- Task 3: Why RDDs are immutable ----------
    println("\n---- Task 3: Why RDDs are immutable ----")
    println("RDDs are immutable because every transformation creates a NEW RDD instead of")
    println("modifying the existing one. This design gives Spark several benefits:")
    println("1. Fault tolerance: since the original data never changes, Spark can always")
    println("   recompute a lost RDD from its lineage (the chain of transformations).")
    println("2. Consistency: multiple tasks can safely read the same RDD in parallel")
    println("   without worrying about another task modifying it mid-computation.")
    println("3. Lineage tracking: because each RDD just records HOW it was derived")
    println("   (not a mutable copy of data), Spark builds a lightweight DAG (graph)")
    println("   instead of duplicating data every step.")

    // ---------- Task 4: How Spark recomputes lost partitions ----------
    println("\n---- Task 4: Recomputing lost partitions ----")
    println("Each RDD stores its lineage: a pointer to its parent RDD(s) plus the")
    println("transformation function used to derive it. If an executor holding a partition")
    println("crashes or is lost, Spark does NOT need to restart the whole job.")
    println("Instead, it looks at the lineage graph, finds which parent partitions are")
    println("needed, and re-runs just the transformations required to rebuild the lost")
    println("partition(s) - reading from the original source or from a cached ancestor.")

    // ---------- Task 5: Scenario - simulate executor loss conceptually ----------
    println("\n---- Task 5: Simulated Executor Loss Scenario ----")
    println(s"Suppose resultRdd has ${resultRdd.getNumPartitions} partitions, and the")
    println("executor holding partition 1 crashes mid-job.")
    println("Spark's DAGScheduler detects the missing partition and checks resultRdd's")
    println("lineage:")
    println("  resultRdd (filter >50) <- doubledRdd (map *2) <- evenRdd (filter even)")
    println("  <- intsRdd (map toInt) <- numbersRdd (textFile numbers.txt)")
    println("Since none of these are cached, Spark recomputes partition 1 from scratch:")
    println("it re-reads the relevant block of numbers.txt and re-applies map -> filter ->")
    println("map -> filter, producing only the missing partition - not the entire dataset.")
    println("If evenRdd had been cached with .cache(), Spark would recompute starting")
    println("from evenRdd instead of all the way back to the text file, saving time.")

    spark.stop()
  }
}
