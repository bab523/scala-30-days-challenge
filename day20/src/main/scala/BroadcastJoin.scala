import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BroadcastJoin {

  // small helper to measure how long an action takes
  def time[T](label: String)(block: => T): T = {
    val start = System.nanoTime()
    val result = block
    val ms = (System.nanoTime() - start) / 1000000
    println(s">>> $label took $ms ms")
    result
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day19 Broadcast Join")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")             // keep plans predictable
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")      // no automatic broadcast
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // ---------- LINE 1: Large fact DataFrame + small reference DataFrame ----------
    val branches = Seq(
      (1, "Hyderabad Main", "Telangana"),
      (2, "Pune Central", "Maharashtra"),
      (3, "Delhi North", "Delhi"),
      (4, "Mumbai West", "Maharashtra"),
      (5, "Chennai South", "Tamil Nadu")
    ).toDF("branch_id", "branch_name", "state")

    // 2 million fake transactions, each assigned to a branch 1..5
    val transactions = spark.range(1, 2000001)
      .select(
        col("id").as("txn_id"),
        (col("id") % 5 + 1).cast("int").as("branch_id"),
        (rand(42) * 10000).cast("int").as("amount")
      )

    println(s"Transactions (large): ${transactions.count()} rows")
    println(s"Branches (small): ${branches.count()} rows")

    // ---------- LINE 2: Broadcast join ----------
    val broadcastJoined = transactions.join(broadcast(branches), "branch_id")

    println("=== Broadcast join plan (look for BroadcastHashJoin, no Exchange on the big side) ===")
    broadcastJoined.explain()

    println("=== Broadcast join result (sample) ===")
    broadcastJoined.show(5)

    // ---------- LINE 4: Compare with shuffle sort merge join ----------
    val sortMergeJoined = transactions.hint("merge").join(branches, "branch_id")

    println("=== Sort merge join plan (look for SortMergeJoin, Exchange, Sort) ===")
    sortMergeJoined.explain()

    // Run both a few times so the comparison is fair (first run includes warm-up)
    time("warm-up")                 { broadcastJoined.count() }
    time("Broadcast join (count)")  { broadcastJoined.count() }
    time("Sort merge join (count)") { sortMergeJoined.count() }

    time("Broadcast join (groupBy)") {
      broadcastJoined.groupBy("branch_name").agg(sum("amount").as("total")).collect()
    }
    time("Sort merge join (groupBy)") {
      sortMergeJoined.groupBy("branch_name").agg(sum("amount").as("total")).collect()
    }

    // ---------- LINE 5: Scenario - millions of transactions + branch master ----------
    println("=== Total amount per branch (2M transactions joined with branch master) ===")
    broadcastJoined
      .groupBy("branch_id", "branch_name", "state")
      .agg(count("*").as("txn_count"), sum("amount").as("total_amount"))
      .orderBy("branch_id")
      .show()

    println("=== Total amount per state ===")
    broadcastJoined
      .groupBy("state")
      .agg(sum("amount").as("total_amount"))
      .orderBy(desc("total_amount"))
      .show()

    // Same with the SQL hint syntax
    transactions.createOrReplaceTempView("transactions")
    branches.createOrReplaceTempView("branches")
    println("=== Same using SQL BROADCAST hint ===")
    spark.sql("""
      SELECT /*+ BROADCAST(b) */ b.branch_name, COUNT(*) AS txn_count, SUM(t.amount) AS total_amount
      FROM transactions t JOIN branches b ON t.branch_id = b.branch_id
      GROUP BY b.branch_name
      ORDER BY b.branch_name
    """).show()

    spark.stop()
  }
}
