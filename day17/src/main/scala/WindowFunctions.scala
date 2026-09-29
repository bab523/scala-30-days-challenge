import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day17 Window Functions")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // 1. row_number, rank, dense_rank per department
    val employees = Seq(
      ("HR", "Asha", 50000), ("HR", "Ravi", 60000), ("HR", "Meena", 60000),
      ("IT", "John", 90000), ("IT", "Sara", 80000), ("IT", "Kiran", 90000),
      ("Sales", "Tom", 40000), ("Sales", "Lily", 45000)
    ).toDF("department", "name", "salary")

    val deptWindow = Window.partitionBy("department").orderBy(col("salary").desc)

    println("=== row_number, rank, dense_rank per department ===")
    employees
      .withColumn("row_number", row_number().over(deptWindow))
      .withColumn("rank", rank().over(deptWindow))
      .withColumn("dense_rank", dense_rank().over(deptWindow))
      .show()

    // 2. Latest policy per customer
    val policies = Seq(
      (1, "P101", "2023-01-10"), (1, "P102", "2024-03-15"), (1, "P103", "2024-11-01"),
      (2, "P201", "2022-05-20"), (2, "P202", "2023-08-30"),
      (3, "P301", "2024-02-14")
    ).toDF("customer_id", "policy_id", "policy_date")
      .withColumn("policy_date", to_date(col("policy_date")))

    val custWindow = Window.partitionBy("customer_id").orderBy(col("policy_date").desc)

    println("=== Latest policy per customer ===")
    policies
      .withColumn("rn", row_number().over(custWindow))
      .filter(col("rn") === 1)
      .drop("rn")
      .show()

    // 3. lag and lead per route
    val trips = Seq(
      ("R1", "2024-01-01", 100), ("R1", "2024-01-02", 120), ("R1", "2024-01-03", 90),
      ("R2", "2024-01-01", 200), ("R2", "2024-01-02", 210), ("R2", "2024-01-03", 190)
    ).toDF("route", "trip_date", "passengers")

    val routeWindow = Window.partitionBy("route").orderBy("trip_date")

    println("=== lag and lead per route ===")
    trips
      .withColumn("previous_day", lag("passengers", 1).over(routeWindow))
      .withColumn("next_day", lead("passengers", 1).over(routeWindow))
      .withColumn("change_from_prev", col("passengers") - col("previous_day"))
      .show()

    // 4. Top 3 students per course
    val students = Seq(
      ("Scala", "Amit", 95), ("Scala", "Bina", 88), ("Scala", "Chetan", 92),
      ("Scala", "Divya", 85), ("Scala", "Esha", 90),
      ("Spark", "Farhan", 78), ("Spark", "Gita", 99), ("Spark", "Hari", 91),
      ("Spark", "Isha", 91), ("Spark", "Jay", 60)
    ).toDF("course", "student", "marks")

    val courseWindow = Window.partitionBy("course").orderBy(col("marks").desc)

    println("=== Top 3 students per course ===")
    students
      .withColumn("rank", dense_rank().over(courseWindow))
      .filter(col("rank") <= 3)
      .show()

    // 5. Same thing using Spark SQL
    students.createOrReplaceTempView("students")
    println("=== Top 3 per course (SQL) ===")
    spark.sql("""
      SELECT * FROM (
        SELECT course, student, marks,
               DENSE_RANK() OVER (PARTITION BY course ORDER BY marks DESC) AS rnk
        FROM students
      ) WHERE rnk <= 3
    """).show()

    spark.stop()
  }
}
