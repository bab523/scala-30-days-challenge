import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day10-Partitioning-App")
      .master("local[4]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ---------- Task 1: Inspect partition counts ----------
    println("---- Task 1: Inspect partition counts ----")

    val customersRdd = sc.textFile("customers.txt")
    println(s"Default partitions for customersRdd (from textFile): ${customersRdd.getNumPartitions}")

    val numbersRdd = sc.parallelize(1 to 20)
    println(s"Default partitions for parallelize(1 to 20): ${numbersRdd.getNumPartitions}")

    val numbersRdd8 = sc.parallelize(1 to 20, 8)
    println(s"Partitions when explicitly set to 8: ${numbersRdd8.getNumPartitions}")

    // Show how many elements are in each partition
    val perPartitionCount = numbersRdd8.mapPartitionsWithIndex { (idx, iter) =>
      Iterator(s"Partition $idx has ${iter.size} elements")
    }
    perPartitionCount.collect().foreach(println)

    // ---------- Task 2: Practice repartition and coalesce ----------
    println("\n---- Task 2: repartition vs coalesce ----")

    val repartitioned = numbersRdd8.repartition(3)
    println(s"After repartition(3): ${repartitioned.getNumPartitions} partitions (involves a full shuffle)")

    val coalesced = numbersRdd8.coalesce(3)
    println(s"After coalesce(3): ${coalesced.getNumPartitions} partitions (avoids full shuffle, merges partitions)")

    val coalescedIncrease = numbersRdd8.coalesce(20)
    println(s"Trying coalesce(20) on an 8-partition RDD: ${coalescedIncrease.getNumPartitions} " +
      "(coalesce CANNOT increase partitions without shuffle=true, so it stays at 8)")

    // ---------- Task 3: When increasing/decreasing partitions helps ----------
    println("\n---- Task 3: When to increase/decrease partitions ----")
    println("INCREASE partitions (repartition) when:")
    println("  - Data is skewed (some partitions much bigger than others)")
    println("  - You need more parallelism to use all available CPU cores")
    println("  - You're about to do a wide/expensive operation and want better load balance")
    println("DECREASE partitions (coalesce) when:")
    println("  - You have too many small partitions causing scheduling overhead")
    println("  - You're writing output and want fewer, larger output files")
    println("  - You just filtered out most of the data and partitions are now mostly empty")
    println("coalesce is preferred for DECREASING because it avoids a full shuffle,")
    println("while repartition always triggers a full shuffle (works for both directions).")

    // ---------- Task 4: partitionBy on a Pair RDD ----------
    println("\n---- Task 4: partitionBy on a Pair RDD ----")

    val cityPairs = customersRdd.map(line => {
      val parts = line.split(",")
      (parts(1), line) // (city, full record)
    })
    println(s"Partitions before partitionBy: ${cityPairs.getNumPartitions}")

    val partitionedByCity = cityPairs.partitionBy(new HashPartitioner(3))
    println(s"Partitions after partitionBy(HashPartitioner(3)): ${partitionedByCity.getNumPartitions}")

    val recordsPerPartition = partitionedByCity.mapPartitionsWithIndex { (idx, iter) =>
      Iterator(s"Partition $idx has ${iter.size} records")
    }
    recordsPerPartition.collect().foreach(println)
    println("partitionBy groups all records with the SAME key into the SAME partition,")
    println("useful before joins/aggregations on that key to avoid extra shuffles later.")

    // ---------- Task 5: Scenario - optimize dataset with too few partitions ----------
    println("\n---- Task 5: Scenario - too few partitions ----")

    val smallPartitionRdd = sc.parallelize(1 to 1000, 1) // Only 1 partition!
    println(s"Problem: large dataset (1000 elements) but only ${smallPartitionRdd.getNumPartitions} partition.")
    println("This means only 1 CPU core does all the work - no parallelism at all.")

    val optimized = smallPartitionRdd.repartition(4)
    println(s"Fix: repartition(4) spreads the data across ${optimized.getNumPartitions} partitions,")
    println("allowing Spark to use up to 4 cores in parallel (matching local[4]).")

    val optimizedCounts = optimized.mapPartitionsWithIndex { (idx, iter) =>
      Iterator(s"Partition $idx now has ${iter.size} elements")
    }
    optimizedCounts.collect().foreach(println)

    spark.stop()
  }
}
