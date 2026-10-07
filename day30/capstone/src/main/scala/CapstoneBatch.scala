import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

case class Order(orderId: String, customerId: String, productId: String,
                 qty: Int, price: Double, date: String, status: String)

object CapstoneBatch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day30-Batch").master("local[*]")
      .config("spark.sql.shuffle.partitions", "8").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // ACCUMULATORS: count bad and cancelled rows without an extra pass
    val badRows   = sc.longAccumulator("badRows")
    val cancelled = sc.longAccumulator("cancelledRows")

    // ---------- 1. RAW + CLEAN (RDD) ----------
    val raw    = sc.textFile("data/orders.csv")
    val header = raw.first()
    val parsed = raw.filter(_ != header).flatMap { line =>
      try {
        val p = line.split(",", -1)
        require(p.length == 7)
        val o = Order(p(0).trim, p(1).trim, p(2).trim, p(3).trim.toInt,
                      p(4).trim.toDouble, p(5).trim, p(6).trim.toLowerCase)
        require(o.orderId.nonEmpty && o.qty > 0 && o.price > 0)
        Some(o)
      } catch { case _: Exception => badRows.add(1); None }
    }

    val clean = parsed.map(o => (o.orderId, o))
      .reduceByKey((a, _) => a)                      // dedupe -> shuffle
      .values
      .filter { o => if (o.status == "cancelled") { cancelled.add(1); false } else true }
      .persist(StorageLevel.MEMORY_AND_DISK)         // PERSIST

    println(s"Clean orders: ${clean.count()}")        // action: accumulators fill here
    println(s"Bad rows (accumulator): ${badRows.value}")
    println(s"Cancelled rows (accumulator): ${cancelled.value}")
    println("---- LINEAGE ----")
    println(clean.toDebugString)

    // ---------- 2. ENRICH (DataFrame + BROADCAST join) ----------
    val customers = spark.read.option("header", "true").csv("data/customers.csv")
    val products  = spark.read.option("header", "true").csv("data/products.csv")
                         .withColumnRenamed("name", "product_name")

    val enriched = clean.toDF()
      .join(F.broadcast(products), $"productId" === $"product_id")
      .join(customers, $"customerId" === $"customer_id")
      .withColumn("order_date", F.to_date($"date"))
      .withColumn("order_month", F.date_format($"order_date", "yyyy-MM"))
      .withColumn("revenue", $"qty" * $"price")
      .withColumn("country", F.upper(F.trim($"country")))
      .drop("date", "product_id", "customer_id")

    // ---------- PARTITION TUNING ----------
    println(s"Partitions before repartition: ${enriched.rdd.getNumPartitions}")
    val base = enriched.repartition(8, $"customerId")
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"Enriched rows: ${base.count()}, partitions after: ${base.rdd.getNumPartitions}")
    base.explain()

    // ---------- 3. SPARK SQL ----------
    base.createOrReplaceTempView("orders_enriched")
    println("Monthly revenue by category (Spark SQL):")
    spark.sql("""
      SELECT order_month, category, ROUND(SUM(revenue),2) AS revenue, COUNT(*) AS orders
      FROM orders_enriched GROUP BY order_month, category
      ORDER BY order_month, revenue DESC""").show(8)

    // ---------- 4. WINDOWS ----------
    val wCat = Window.partitionBy("category").orderBy($"cat_revenue".desc)
    val topProducts = base.groupBy("category", "product_name")
      .agg(F.sum("revenue").as("cat_revenue"))
      .withColumn("rank", F.dense_rank().over(wCat))
      .filter($"rank" <= 3)
    topProducts.orderBy("category", "rank").show(12, false)

    val wCust = Window.partitionBy("customerId").orderBy("order_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    base.withColumn("running_spend", F.sum("revenue").over(wCust))
      .select("customerId", "order_date", "revenue",
cat > src/main/scala/CapstoneStreaming.scala <<'EOF'
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object CapstoneStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day30-Streaming").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")   // default 200 is too many for streaming state
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val schema = new StructType()
      .add("order_id", StringType).add("customer_id", StringType)
      .add("product_id", StringType).add("qty", IntegerType)
      .add("price", DoubleType).add("event_time", StringType)

    val products = spark.read.option("header", "true").csv("data/products.csv")

    val stream = spark.readStream.schema(schema)
      .option("maxFilesPerTrigger", "1")             // one file per micro-batch
      .csv("stream_in")
      .withColumn("event_time", F.to_timestamp($"event_time"))
      .filter($"qty" > 0 && $"price" > 0)
      .withColumn("revenue", $"qty" * $"price")

    // stream-static join, products table broadcast
    val enriched = stream.join(F.broadcast(products), "product_id")

    // windowed aggregation with watermark (state is cleaned after 1 minute)
    val agg = enriched.withWatermark("event_time", "1 minute")
      .groupBy(F.window($"event_time", "30 seconds"), $"category")
      .agg(F.sum("revenue").as("revenue"), F.count("*").as("orders"))

    val query = agg.writeStream
      .outputMode("update")
      .format("console").option("truncate", "false")
      .option("checkpointLocation", "checkpoint/stream")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination(80000)   // auto stop after 80 sec
    query.stop(); spark.stop()
  }
}
