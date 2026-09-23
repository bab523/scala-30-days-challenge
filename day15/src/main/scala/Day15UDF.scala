import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15UDF {

  def timed[T](label: String)(block: => T): T = {
    val t0 = System.nanoTime()
    val r = block
    println(f"$label%-28s -> ${(System.nanoTime() - t0) / 1e6}%.1f ms")
    r
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day15").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val emp = spark.read.option("header", "true").option("inferSchema", "true").csv("data/employees.csv")
    val txn = spark.read.option("header", "true").option("inferSchema", "true").csv("data/transactions.csv")

    println("\n=== TASK 1: Scala UDF for salary bands ===")
    val salaryBand = udf((salary: Double) =>
      if (salary >= 100000) "HIGH" else if (salary >= 50000) "MEDIUM" else "LOW")
    emp.select(col("name"), col("salary"), salaryBand(col("salary").cast("double")).as("band")).show()

    println("\n=== TASK 2: calculated columns with withColumn ===")
    val enriched = emp
      .withColumn("bonus", round(col("salary") * col("bonusPct") / 100, 2))
      .withColumn("total_comp", col("salary") + col("bonus"))
      .withColumn("band", salaryBand(col("salary").cast("double")))
      .withColumn("name_upper", upper(col("name")))
    enriched.show()

    println("\n=== TASK 3: UDF vs built-in function ===")
    val builtinBand = when(col("salary") >= 100000, "HIGH")
      .when(col("salary") >= 50000, "MEDIUM")
      .otherwise("LOW")
    val cmp = emp
      .withColumn("udf_band", salaryBand(col("salary").cast("double")))
      .withColumn("builtin_band", builtinBand)
    cmp.select("name", "salary", "udf_band", "builtin_band").show()
    println(s"Rows where UDF and built-in differ: ${cmp.filter(col("udf_band") =!= col("builtin_band")).count()}")

    println("--- Plan with UDF (black box for Catalyst) ---")
    emp.withColumn("band", salaryBand(col("salary").cast("double"))).explain()
    println("--- Plan with built-in (Catalyst sees CASE WHEN) ---")
    emp.withColumn("band", builtinBand).explain()

    val big = spark.range(3000000).withColumn("salary", (col("id") % 150000).cast("double"))
    big.withColumn("band", salaryBand(col("salary"))).filter(col("band") === "HIGH").count() // warm-up
    timed("3M rows with UDF")(big.withColumn("band", salaryBand(col("salary"))).filter(col("band") === "HIGH").count())
    timed("3M rows with built-in")(big.withColumn("band", builtinBand).filter(col("band") === "HIGH").count())

    println("\n=== TASK 4: register UDF with session/catalog ===")
    spark.udf.register("salary_band_sql", (salary: Double) =>
      if (salary >= 100000) "HIGH" else if (salary >= 50000) "MEDIUM" else "LOW")
    emp.createOrReplaceTempView("employees")
    spark.sql("SELECT name, salary, salary_band_sql(salary) AS band FROM employees ORDER BY salary DESC").show()
    println(s"Registered in catalog? ${spark.catalog.functionExists("salary_band_sql")}")
    spark.catalog.listFunctions().filter(col("name") === "salary_band_sql").select("name", "isTemporary").show()

    println("\n=== TASK 5: customer risk category from transaction values ===")
    val riskCategory = udf((total: Double, maxTxn: Double, cnt: Long) =>
      if (total >= 50000 || maxTxn >= 40000) "HIGH"
      else if (total >= 5000 || maxTxn >= 1000 || cnt >= 10) "MEDIUM"
      else "LOW")

    val perCustomer = txn.groupBy("customer_id").agg(
      count("txn_id").as("txn_count"),
      round(sum("amount"), 2).as("total_amount"),
      max("amount").as("max_txn"))
    val risk = perCustomer.withColumn("risk",
      riskCategory(col("total_amount"), col("max_txn"), col("txn_count")))

    println("Report 1: risk category per customer")
    risk.orderBy(desc("total_amount")).show()

    println("Report 2: customers per risk category")
    risk.groupBy("risk").count().orderBy("risk").show()

    println("Report 3: HIGH risk customers to review")
    risk.filter(col("risk") === "HIGH").select("customer_id", "total_amount", "max_txn").show()

    spark.stop()
  }
}
