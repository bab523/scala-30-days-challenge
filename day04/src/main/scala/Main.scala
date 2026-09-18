import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day4-RDD-App")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR") // reduce noisy logs

    // ---------- Task 1: Create RDDs from collections and text files ----------
    println("---- Task 1: RDD Creation ----")

    // From a Scala collection
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numRdd = sc.parallelize(numbers)
    println(s"RDD from collection: ${numRdd.collect().mkString(", ")}")

    // From a text file
    val txnRdd = sc.textFile("transactions.txt")
    println(s"RDD from text file, first 3 lines: ${txnRdd.take(3).mkString(" | ")}")

    // ---------- Task 2: map, filter, flatMap on RDD ----------
    println("\n---- Task 2: map, filter, flatMap ----")

    val squared = numRdd.map(x => x * x)
    println(s"Squared numbers: ${squared.collect().mkString(", ")}")

    val evens = numRdd.filter(x => x % 2 == 0)
    println(s"Even numbers: ${evens.collect().mkString(", ")}")

    val words = txnRdd.flatMap(line => line.split(","))
    println(s"Flattened words (first 6): ${words.take(6).mkString(", ")}")

    // ---------- Task 3: Calculate total sales from transaction records ----------
    println("\n---- Task 3: Total sales ----")

    // Each line: customerId,product,price
    val salesRdd = txnRdd.map(line => line.split(",")(2).toDouble)
    val totalSales = salesRdd.sum()
    println(s"Total sales: $totalSales")

    // Sales grouped by customer
    val customerSales = txnRdd
      .map(line => {
        val parts = line.split(",")
        (parts(0), parts(2).toDouble)
      })
      .reduceByKey(_ + _)
    println("Sales by customer:")
    customerSales.collect().foreach { case (cust, total) => println(s"  $cust -> $total") }

    // ---------- Task 4: Inspect partitions and explain default parallelism ----------
    println("\n---- Task 4: Partitions ----")
    println(s"Number of partitions in numRdd: ${numRdd.getNumPartitions}")
    println(s"Number of partitions in txnRdd: ${txnRdd.getNumPartitions}")
    println(s"Default parallelism (from SparkContext): ${sc.defaultParallelism}")
    println("Explanation: Default parallelism usually equals the number of cores " +
      "available to Spark (here local[2] gives 2). For files, the number of partitions " +
      "also depends on file size and HDFS/local block size, with a minimum split count.")

    // ---------- Task 5: Scenario - process large customer file split into multiple partitions ----------
    println("\n---- Task 5: Repartitioning scenario ----")
    val repartitioned = txnRdd.repartition(4)
    println(s"Partitions after repartition(4): ${repartitioned.getNumPartitions}")

    val salesPerPartition = repartitioned.mapPartitionsWithIndex { (idx, iter) =>
      val lines = iter.toList
      Iterator(s"Partition $idx has ${lines.size} records")
    }
    salesPerPartition.collect().foreach(println)

    spark.stop()
  }
}
