import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day18 Joins")
      .master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1") // disable broadcast so we can see sort merge join
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // ---------- Data ----------
    val customers = Seq(
      (1, "Asha", "Hyderabad"), (2, "Ravi", "Pune"),
      (3, "Meena", "Delhi"), (4, "John", "Mumbai")
    ).toDF("id", "name", "city")

    val orders = Seq(
      (101, 1, 500), (102, 1, 300), (103, 2, 700), (104, 5, 200) // customer 5 does not exist
    ).toDF("id", "customer_id", "amount")

    val payments = Seq(
      (1001, 101, "PAID"), (1002, 102, "PAID"),
      (1003, 103, "PENDING"), (1004, 999, "PAID") // order 999 does not exist
    ).toDF("id", "order_id", "status")

    // ---------- 1. Inner, left, right, full joins ----------
    // Both tables have a column called "id" -> ambiguous, so we use aliases c and o
    val cond = col("c.id") === col("o.customer_id")
    val c = customers.as("c")
    val o = orders.as("o")

    def pick(df: org.apache.spark.sql.DataFrame) =
      df.select(
        col("c.id").as("customer_id"), col("c.name"),
        col("o.id").as("order_id"), col("o.amount")
      )

    println("=== INNER JOIN ===")
    pick(c.join(o, cond, "inner")).show()

    println("=== LEFT JOIN (all customers) ===")
    pick(c.join(o, cond, "left")).show()

    println("=== RIGHT JOIN (all orders) ===")
    pick(c.join(o, cond, "right")).show()

    println("=== FULL OUTER JOIN ===")
    pick(c.join(o, cond, "full")).show()

    // ---------- 2. Ambiguous column names ----------
    // customers.join(orders, customers("id") === orders("customer_id")).select("id")
    // would FAIL with "Reference 'id' is ambiguous". Fix = aliases (done above)
    // or renaming columns before the join:
    println("=== Fix ambiguity by renaming ===")
    val ordersRenamed = orders.withColumnRenamed("id", "order_id")
    customers.join(ordersRenamed, customers("id") === ordersRenamed("customer_id"), "inner")
      .select(customers("id"), col("name"), col("order_id"), col("amount"))
      .show()

    // ---------- 3. Null handling after left join ----------
    val leftJoined = pick(c.join(o, cond, "left"))

    println("=== Customers with NO orders (isNull) ===")
    leftJoined.filter(col("order_id").isNull).show()

    println("=== coalesce / na.fill ===")
    leftJoined
      .withColumn("amount", coalesce(col("amount"), lit(0)))
      .withColumn("order_status", when(col("order_id").isNull, "NO ORDER").otherwise("HAS ORDER"))
      .show()

    leftJoined.na.fill(Map("amount" -> 0, "order_id" -> -1)).show()

    // ---------- 4. Shuffle sort merge join ----------
    println("=== Physical plan (look for SortMergeJoin, Exchange, Sort) ===")
    customers.join(orders, customers("id") === orders("customer_id"), "inner").explain()

    println("=== Same, forcing it with a hint ===")
    customers.hint("merge").join(orders, customers("id") === orders("customer_id")).explain()

    // ---------- 5. Scenario: orders + customers + payments ----------
    val full = c
      .join(o, col("c.id") === col("o.customer_id"), "left")
      .join(payments.as("p"), col("o.id") === col("p.order_id"), "left")

    println("=== Customer -> Order -> Payment ===")
    full.select(
      col("c.name"), col("o.id").as("order_id"), col("o.amount"),
      coalesce(col("p.status"), lit("NO PAYMENT")).as("payment_status")
    ).show()

    println("=== Paid amount per customer ===")
    full.groupBy(col("c.name"))
      .agg(
        count(col("o.id")).as("total_orders"),
        sum(when(col("p.status") === "PAID", col("o.amount")).otherwise(0)).as("paid_amount")
      ).show()

    spark.stop()
  }
}
