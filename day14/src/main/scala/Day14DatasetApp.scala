import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

// Case classes hamesha object ke BAHAR (top level) rakhte hain, taaki Spark encoder bana sake
case class Employee(id: Int, name: String, dept: String, salary: Double, bonusPct: Double)
case class Payroll(id: Int, name: String, dept: String, gross: Double, tax: Double, net: Double)

object Day14DatasetApp {

  def r2(x: Double): Double = math.round(x * 100) / 100.0

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day14").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("\n=== TASK 1: case class + DataFrame -> Dataset ===")
    val empDF = spark.read
      .option("header", "true")
      .schema(Encoders.product[Employee].schema)
      .csv("data/employees.csv")
    println("DataFrame (untyped, rows of Row):")
    empDF.show()

    val empDS: Dataset[Employee] = empDF.as[Employee]
    println("Dataset[Employee] (typed objects):")
    empDS.show()
    println(s"First employee object: ${empDS.first()}")

    println("\n=== TASK 2: Dataset -> DataFrame ===")
    val backToDF = empDS.toDF()
    backToDF.printSchema()
    println(s"Type of empDS     : Dataset[Employee]")
    println(s"Type of backToDF  : DataFrame = Dataset[Row], columns: ${backToDF.columns.mkString(", ")}")

    println("\n=== TASK 3: compare RDD, DataFrame, Dataset ===")
    println(s"RDD       filter (salary > 60000): ${empDS.rdd.filter(e => e.salary > 60000).count()}")
    println(s"DataFrame filter (salary > 60000): ${empDF.filter(col("salary") > 60000).count()}")
    println(s"Dataset   filter (salary > 60000): ${empDS.filter(e => e.salary > 60000).count()}")
    println()
    println("RDD       : typed objects, NO Catalyst optimizer, no schema, you write all the logic.")
    println("DataFrame : Row + schema, Catalyst optimizes, but column names are checked only at RUNTIME.")
    println("Dataset   : typed objects + schema + Catalyst, mistakes in field names fail at COMPILE time.")

    println("\n=== TASK 4: type safety and Catalyst ===")
    val bad = scala.util.Try(empDF.select("salry"))
    println(s"DataFrame with typo column 'salry' -> ${if (bad.isFailure) "FAILED at runtime: " + bad.failed.get.getClass.getSimpleName else "ok"}")
    println("Dataset with typo e.salry -> would not even compile (compile-time safety).")

    println("\n--- Plan: DataFrame column expression (Catalyst can push the filter down) ---")
    empDF.filter(col("salary") > 60000).select("name").explain()

    println("--- Plan: Dataset lambda (opaque to Catalyst, no pushed filter) ---")
    empDS.filter(e => e.salary > 60000).select("name").explain()

    println("\n=== TASK 5: typed employee payroll pipeline ===")
    val payroll: Dataset[Payroll] = empDS.map { e =>
      val gross = e.salary + e.salary * e.bonusPct / 100
      val taxRate = if (gross > 100000) 0.30 else if (gross > 50000) 0.20 else 0.10
      val tax = gross * taxRate
      Payroll(e.id, e.name, e.dept, r2(gross), r2(tax), r2(gross - tax))
    }
    println("Payroll for every employee:")
    payroll.show()

    println("Employees with net pay above 60000 (typed filter):")
    payroll.filter(p => p.net > 60000).show()

    println("Department summary:")
    payroll.groupBy("dept")
      .agg(count("id").as("employees"), round(sum("net"), 2).as("total_net"))
      .orderBy(desc("total_net"))
      .show()

    println(s"Highest net pay: ${payroll.orderBy(desc("net")).first()}")

    spark.stop()
  }
}
