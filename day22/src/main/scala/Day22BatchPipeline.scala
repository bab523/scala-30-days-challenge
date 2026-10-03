import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object Day22BatchPipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day22BatchPipeline")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val dateRegex = "^\\d{4}-\\d{2}-\\d{2}$"

    // ---------- STEP 1: Read raw data (sab columns string me) ----------
    def readCsv(path: String) =
      spark.read.option("header", "true").csv(path)

    val rawTxn   = readCsv("data/raw/transactions.csv")
    val customers = readCsv("data/raw/customers.csv")
    val products  = readCsv("data/raw/products.csv")
      .withColumn("unit_price", $"unit_price".cast("double"))

    println(s"Raw transactions count: ${rawTxn.count()}")
    rawTxn.show(false)

    // ---------- STEP 2: Duplicate hatao ----------
    val deduped = rawTxn.dropDuplicates("txn_id")
    println(s"After removing duplicates: ${deduped.count()}")

    // ---------- STEP 3: Join customer + product (left join, taaki unknown ids pakde jaye) ----------
    val joined = deduped
      .join(customers, Seq("customer_id"), "left")
      .join(products,  Seq("product_id"),  "left")

    // ---------- STEP 4: Validation, har kharab row ko reason do ----------
    val checked = joined.withColumn("reject_reason",
      when($"customer_id".isNull || trim($"customer_id") === "", "missing_customer")
      .when($"customer_name".isNull, "unknown_customer")
      .when($"product_name".isNull, "unknown_product")
      .when($"qty".cast("int").isNull || $"qty".cast("int") <= 0, "invalid_qty")
      .when($"txn_date".isNull || !$"txn_date".rlike(dateRegex), "invalid_date")
      .when($"payment_status" =!= "PAID", "payment_not_paid")
    )

    val rejected = checked.filter($"reject_reason".isNotNull)
    val valid    = checked.filter($"reject_reason".isNull)

    println("=== Rejected records ===")
    rejected.select("txn_id", "customer_id", "product_id", "qty", "txn_date", "payment_status", "reject_reason").show(false)
    println("=== Reject reason summary ===")
    rejected.groupBy("reject_reason").count().show()

    rejected.drop("customer_name", "city", "segment", "product_name", "category", "unit_price")
      .coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("output/rejected")

    // ---------- STEP 5: Clean data ko sahi types do + revenue nikalo ----------
    val clean = valid
      .withColumn("qty", $"qty".cast("int"))
      .withColumn("txn_date", to_date($"txn_date"))
      .withColumn("revenue", $"qty" * $"unit_price")
      .withColumn("year",  year($"txn_date"))
      .withColumn("month", month($"txn_date"))

    println("=== Clean enriched records ===")
    clean.select("txn_id", "txn_date", "customer_name", "city", "product_name", "category", "qty", "revenue").show(false)

    // ---------- STEP 6: Aggregate daily revenue ----------
    val dailySales = clean
      .groupBy($"year", $"month", $"txn_date", $"category")
      .agg(
        countDistinct($"txn_id").as("orders"),
        sum($"qty").as("units_sold"),
        sum($"revenue").as("total_revenue")
      )
      .orderBy($"txn_date", $"category")

    println("=== Daily sales by category ===")
    dailySales.show(false)

    // City-wise summary (sirf dekhne ke liye)
    println("=== Revenue by city ===")
    clean.groupBy("city").agg(sum("revenue").as("total_revenue")).orderBy(desc("total_revenue")).show()

    // ---------- STEP 7: Partitioned Parquet likho ----------
    dailySales
      .repartition($"year", $"month")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month")
      .parquet("output/daily_sales")

    // ---------- STEP 8: Wapas padh ke verify karo ----------
    println("=== Read back from Parquet (only Jan 2025, partition pruning) ===")
    spark.read.parquet("output/daily_sales")
      .filter($"year" === 2025 && $"month" === 1)
      .show(false)

    println(s"Summary -> raw: ${rawTxn.count()}, valid: ${valid.count()}, rejected: ${rejected.count()}")

    spark.stop()
  }
}
