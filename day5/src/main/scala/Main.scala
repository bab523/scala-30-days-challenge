import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day5-Transformations-App")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ---------- Task 1: map, filter, flatMap, distinct, union ----------
    println("---- Task 1: map, filter, flatMap, distinct, union ----")

    val nums1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val nums2 = sc.parallelize(List(4, 5, 6, 7, 8))

    val mapped = nums1.map(x => x * 10)
    println(s"map result: ${mapped.collect().mkString(", ")}")

    val filtered = nums1.filter(x => x % 2 == 0)
    println(s"filter result: ${filtered.collect().mkString(", ")}")

    val flatMapped = nums1.flatMap(x => List(x, x * 100))
    println(s"flatMap result: ${flatMapped.collect().mkString(", ")}")

    val unioned = nums1.union(nums2)
    println(s"union result: ${unioned.collect().mkString(", ")}")

    val distinctVals = unioned.distinct()
    println(s"distinct result: ${distinctVals.collect().mkString(", ")}")

    // ---------- Task 2: count, collect, first, take, reduce ----------
    println("\n---- Task 2: count, collect, first, take, reduce ----")

    println(s"count: ${nums1.count()}")
    println(s"collect: ${nums1.collect().mkString(", ")}")
    println(s"first: ${nums1.first()}")
    println(s"take(3): ${nums1.take(3).mkString(", ")}")
    println(s"reduce (sum): ${nums1.reduce((a, b) => a + b)}")

    // ---------- Task 3: Transformations vs Actions ----------
    println("\n---- Task 3: Transformations vs Actions ----")
    println("Transformations (lazy, return a new RDD, not executed until an action is called):")
    println("  map, filter, flatMap, distinct, union, reduceByKey, repartition")
    println("Actions (trigger actual computation, return a value or write output):")
    println("  count, collect, first, take, reduce, foreach, saveAsTextFile")

    // ---------- Task 4: Identify which operations are lazy ----------
    println("\n---- Task 4: Lazy operations ----")
    println("map, filter, flatMap, distinct and union are all LAZY. Spark just builds up a")
    println("logical plan (DAG) for these and does not compute anything until an action")
    println("like collect() or count() is called. This lets Spark optimize the execution plan.")

    // ---------- Task 5: Scenario - log analyzer counting ERROR messages ----------
    println("\n---- Task 5: Log analyzer (count ERROR messages) ----")

    val logsRdd = sc.textFile("logs.txt")
    val errorLines = logsRdd.filter(line => line.startsWith("ERROR")) // transformation (lazy)
    val errorCount = errorLines.count() // action (triggers execution)

    println(s"Total ERROR messages: $errorCount")
    println("ERROR lines:")
    errorLines.collect().foreach(line => println(s"  $line"))

    // Bonus: count by log level
    val levelCounts = logsRdd
      .map(line => line.split(" ")(0))
      .map(level => (level, 1))
      .reduceByKey(_ + _)
    println("\nLog level counts:")
    levelCounts.collect().foreach { case (level, cnt) => println(s"  $level -> $cnt") }

    spark.stop()
  }
}
