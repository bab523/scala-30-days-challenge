import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16Aggregations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day16").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val emp = spark.read.option("header", "true").option("inferSchema", "true").csv("data/employees.csv")
    val bills = spark.read.option("header", "true").option("inferSchema", "true").csv("data/hospital_billing.csv")

    println("\n=== TASK 1: count, sum, avg, min, max ===")
    emp.agg(
      count("id").as("employees"),
      sum("salary").as("total_salary"),
      round(avg("salary"), 2).as("avg_salary"),
      min("salary").as("min_salary"),
      max("salary").as("max_salary")
    ).show()
    emp.agg(countDistinct("dept").as("distinct_departments")).show()

    println("\n=== TASK 2: groupBy with multiple columns ===")
    println("Employees by dept and gender:")
    emp.groupBy("dept", "gender")
      .agg(count("id").as("employees"), round(avg("salary"), 2).as("avg_salary"))
      .orderBy("dept", "gender")
      .show()

    println("Hospital revenue by department and month:")
    bills.groupBy("department", "month")
      .agg(sum("amount").as("revenue"), sum("patients").as("patients"))
      .orderBy("department", "month")
      .show(20)

    println("\n=== TASK 3: HAVING-like filtering after aggregation ===")
    println("DataFrame way: groupBy -> agg -> filter (departments with avg salary > 60000)")
    emp.groupBy("dept")
      .agg(round(avg("salary"), 2).as("avg_salary"))
      .filter(col("avg_salary") > 60000)
      .show()

    println("SQL way: GROUP BY ... HAVING")
    emp.createOrReplaceTempView("employees")
    spark.sql(
      """SELECT dept, COUNT(*) AS employees, ROUND(AVG(salary), 2) AS avg_salary
         FROM employees
         GROUP BY dept
         HAVING AVG(salary) > 60000 AND COUNT(*) >= 3""").show()

    println("WHERE filters rows BEFORE grouping, HAVING filters groups AFTER grouping.")
    println("Only female employees per dept (WHERE first, then group):")
    emp.filter(col("gender") === "F").groupBy("dept").count().orderBy("dept").show()

    println("\n=== TASK 4: department-wise salary statistics ===")
    emp.groupBy("dept").agg(
      count("id").as("employees"),
      sum("salary").as("total_salary"),
      round(avg("salary"), 2).as("avg_salary"),
      min("salary").as("min_salary"),
      max("salary").as("max_salary"),
      (max("salary") - min("salary")).as("salary_gap"),
      round(stddev("salary"), 2).as("std_dev")
    ).orderBy(desc("avg_salary")).show()

    println("\n=== TASK 5: hospital department revenue metrics ===")
    val totalRevenue = bills.agg(sum("amount")).first().getAs[Number](0).doubleValue()
    println(s"Total hospital revenue: $totalRevenue")

    val dept = bills.groupBy("department").agg(
        count("bill_id").as("bills"),
        sum("patients").as("patients"),
        sum("amount").as("revenue"),
        round(avg("amount"), 2).as("avg_bill"),
        max("amount").as("max_bill"))
      .withColumn("revenue_per_patient", round(col("revenue") / col("patients"), 2))
      .withColumn("share_pct", round(col("revenue") * 100 / totalRevenue, 2))

    println("Report 1: department revenue metrics")
    dept.orderBy(desc("revenue")).show()

    println("Report 2: monthly revenue by department (columns 1, 2, 3 = month number)")
    bills.groupBy("department").pivot("month").sum("amount").na.fill(0).orderBy("department").show()

    println("Report 3: departments with revenue above 1,000,000 (HAVING)")
    bills.createOrReplaceTempView("billing")
    spark.sql(
      """SELECT department, SUM(amount) AS revenue, SUM(patients) AS patients
         FROM billing
         GROUP BY department
         HAVING SUM(amount) > 1000000
         ORDER BY revenue DESC""").show()

    println("Report 4: OPD vs IPD revenue per department")
    bills.groupBy("department", "service_type")
      .agg(sum("amount").as("revenue"), sum("patients").as("patients"))
      .orderBy("department", "service_type")
      .show()

    println(s"Top department by revenue: ${dept.orderBy(desc("revenue")).first()}")

    spark.stop()
  }
}
