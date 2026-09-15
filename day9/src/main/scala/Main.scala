import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day9-PairRDD-App")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ---------- Task 1: Create key-value (Pair) RDDs ----------
    println("---- Task 1: Create key-value RDDs ----")

    val salesRdd = sc.textFile("sales.txt")
    val productRevenuePair = salesRdd.map(line => {
      val parts = line.split(",")
      (parts(0), parts(2).toDouble) // (product, revenue)
    })
    val deptRevenuePair = salesRdd.map(line => {
      val parts = line.split(",")
      (parts(1), parts(2).toDouble) // (department, revenue)
    })

    println("Product-Revenue pairs (first 5):")
    productRevenuePair.take(5).foreach(println)

    // ---------- Task 2: reduceByKey, groupByKey, mapValues ----------
    println("\n---- Task 2: reduceByKey, groupByKey, mapValues ----")

    val revenueByProductReduce = productRevenuePair.reduceByKey(_ + _)
    println("Using reduceByKey (product -> total revenue):")
    revenueByProductReduce.collect().foreach { case (p, r) => println(s"  $p -> $r") }

    val revenueByProductGroup = productRevenuePair.groupByKey()
    println("\nUsing groupByKey (product -> all values grouped):")
    revenueByProductGroup.collect().foreach { case (p, vals) =>
      println(s"  $p -> ${vals.toList}")
    }

    val taxedRevenue = productRevenuePair.mapValues(v => v * 1.18)
    println("\nUsing mapValues (add 18% tax to each revenue value):")
    taxedRevenue.take(5).foreach(println)

    // ---------- Task 3: Revenue by product and department ----------
    println("\n---- Task 3: Revenue by product and department ----")

    println("Revenue by product:")
    revenueByProductReduce.collect().foreach { case (p, r) => println(s"  $p -> $r") }

    val revenueByDept = deptRevenuePair.reduceByKey(_ + _)
    println("\nRevenue by department:")
    revenueByDept.collect().foreach { case (d, r) => println(s"  $d -> $r") }

    // ---------- Task 4: Compare reduceByKey vs groupByKey performance ----------
    println("\n---- Task 4: reduceByKey vs groupByKey performance ----")
    println("reduceByKey: combines values on EACH partition FIRST (map-side combine/")
    println("  pre-aggregation), then shuffles only the already-reduced results across")
    println("  the network. This means LESS data movement and better performance.")
    println("groupByKey: shuffles ALL raw values across the network first, then groups")
    println("  them by key. This sends much more data over the network and uses more")
    println("  memory, especially with large datasets or skewed keys.")
    println("Conclusion: prefer reduceByKey (or aggregateByKey/combineByKey) over")
    println("  groupByKey whenever you just need an aggregate result like a sum.")

    // ---------- Task 5: Scenario - aggregate bank transactions by account ID ----------
    println("\n---- Task 5: Aggregate bank transactions by account ID ----")

    val txnRdd = sc.textFile("transactions.txt")
    val accountPairs = txnRdd.map(line => {
      val parts = line.split(",")
      (parts(0), parts(1).toDouble) // (accountId, amount)
    })

    val accountBalances = accountPairs.reduceByKey(_ + _)
    println("Final balance per account (sum of all transactions):")
    accountBalances.collect().sortBy(_._1).foreach { case (acc, balance) =>
      println(s"  $acc -> $balance")
    }

    val txnCountPerAccount = accountPairs.mapValues(_ => 1).reduceByKey(_ + _)
    println("\nNumber of transactions per account:")
    txnCountPerAccount.collect().sortBy(_._1).foreach { case (acc, cnt) =>
      println(s"  $acc -> $cnt transactions")
    }

    spark.stop()
  }
}
