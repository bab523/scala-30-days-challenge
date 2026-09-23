import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day13SparkSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day13").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println("\n=== TASK 1: DataFrame from CSV and JSON ===")
    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")
    val orders = spark.read.json("data/orders.json")
    customers.show()
    orders.show()

    println("\n=== TASK 2: schema, select, filter ===")
    customers.printSchema()
    orders.printSchema()
    println("select name, city:")
    customers.select("name", "city").show()
    println("customers with age > 30:")
    customers.filter(col("age") > 30).show()
    println("Electronics orders above 500:")
    orders.filter(col("amount") > 500 && col("category") === "Electronics").show()

    println("\n=== TASK 3: withColumn and expressions ===")
    val ordersEnriched = orders
      .withColumn("gst", round(col("amount") * 0.18, 2))
      .withColumn("total", round(col("amount") + col("gst"), 2))
      .withColumn("order_size",
        when(col("amount") >= 1000, "HIGH")
          .when(col("amount") >= 300, "MEDIUM")
          .otherwise("LOW"))
    ordersEnriched.show()

    val customersEnriched = customers
      .withColumn("age_group", when(col("age") < 30, "Young").otherwise("Adult"))
      .withColumn("name_upper", upper(col("name")))
    customersEnriched.show()

    println("\n=== TASK 4: temp view and SQL ===")
    customers.createOrReplaceTempView("customers")
    orders.createOrReplaceTempView("orders")
    spark.sql(
      """SELECT category, COUNT(*) AS num_orders, ROUND(SUM(amount), 2) AS revenue
         FROM orders
         GROUP BY category
         ORDER BY revenue DESC""").show()

    println("\n=== TASK 5: customer analytics report ===")
    println("Report 1: per-customer summary (left join keeps customers with no orders)")
    spark.sql(
      """SELECT c.customer_id, c.name, c.city,
                COUNT(o.order_id) AS total_orders,
                ROUND(COALESCE(SUM(o.amount), 0), 2) AS total_spent,
                ROUND(COALESCE(AVG(o.amount), 0), 2) AS avg_order
         FROM customers c
         LEFT JOIN orders o ON c.customer_id = o.customer_id
         GROUP BY c.customer_id, c.name, c.city
         ORDER BY total_spent DESC""").show()

    println("Report 2: revenue by city")
    spark.sql(
      """SELECT c.city, COUNT(DISTINCT c.customer_id) AS customers,
                ROUND(COALESCE(SUM(o.amount), 0), 2) AS revenue
         FROM customers c
         LEFT JOIN orders o ON c.customer_id = o.customer_id
         GROUP BY c.city
         ORDER BY revenue DESC""").show()

    println("Report 3: customers with no orders")
    spark.sql(
      """SELECT c.customer_id, c.name
         FROM customers c
         LEFT JOIN orders o ON c.customer_id = o.customer_id
         WHERE o.order_id IS NULL""").show()

    spark.stop()
  }
}
