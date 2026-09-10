import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    // ---------- Task 1 & 2: Create Scala Spark project with SparkSession/SparkContext ----------
    println("---- Task 1 & 2: SparkSession and SparkContext ----")

    val spark = SparkSession.builder()
      .appName("Day3-SparkApp")
      .master("local[2]") // local mode with 2 cores
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    println(s"Spark version: ${spark.version}")
    println(s"App Name: ${sc.appName}")
    println(s"Master: ${sc.master}")

    // ---------- Task 3: Read a text file and display its contents ----------
    println("\n---- Task 3: Reading text file ----")
    val rdd = sc.textFile("sample.txt")
    rdd.collect().foreach(println)

    // ---------- Task 4: Explain driver, executor and cluster manager ----------
    println("\n---- Task 4: Driver, Executor, Cluster Manager ----")
    println("Driver: The process that runs the main() function and creates SparkContext. " +
      "It coordinates the execution of the Spark application.")
    println("Executor: Worker processes launched on cluster nodes that run tasks and store data " +
      "for the application.")
    println("Cluster Manager: Allocates resources across applications (e.g., local, standalone, YARN, Kubernetes). " +
      "Here we use 'local' as the cluster manager since we're running on our own machine.")

    // ---------- Task 5: Scenario - run same app with 2 and 4 cores ----------
    println("\n---- Task 5: Running with different core counts ----")
    println(s"Currently running with master = ${sc.master}")
    println("To run with 4 cores instead, change .master(\"local[2]\") to .master(\"local[4]\") " +
      "and rerun the application. This demonstrates how Spark can scale parallelism " +
      "based on available cores without changing the core logic.")

    spark.stop()
  }
}
