import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Day12CachePersist {

  case class Txn(id: Int, customer: String, category: String, amount: Double)

  def timed[T](label: String)(block: => T): T = {
    val t0 = System.nanoTime()
    val r = block
    println(f"$label%-38s -> ${(System.nanoTime() - t0) / 1e6}%.1f ms")
    r
  }

  def slow(x: Int): Long = {
    var s = 0L; var i = 0
    while (i < 2000) { s += (x.toLong * i) % 7; i += 1 }
    s
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day12").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val acc = sc.longAccumulator("computations")
    def buildRdd() = sc.parallelize(1 to 200000, 4).map { x => acc.add(1); slow(x) }

    println("\n=== TASK 1: cache ===")
    val noCache = buildRdd()
    timed("NO cache - action 1 (count)")(noCache.count())
    timed("NO cache - action 2 (sum)")(noCache.sum())
    println(s"Elements computed WITHOUT cache: ${acc.value}")

    acc.reset()
    val cached = buildRdd().cache()
    timed("cache - action 1 (count)")(cached.count())
    timed("cache - action 2 (sum)")(cached.sum())
    println(s"Elements computed WITH cache: ${acc.value}")

    println("\n=== TASK 2: cache vs persist ===")
    val a = buildRdd().cache()
    val b = buildRdd().persist(StorageLevel.MEMORY_ONLY)
    println(s"cache()   level : ${a.getStorageLevel.description}")
    println(s"persist() level : ${b.getStorageLevel.description}")
    println("cache() == persist(MEMORY_ONLY) for RDD")

    println("\n=== TASK 3: storage levels ===")
    val levels = Seq(
      "MEMORY_ONLY"         -> StorageLevel.MEMORY_ONLY,
      "MEMORY_AND_DISK"     -> StorageLevel.MEMORY_AND_DISK,
      "MEMORY_ONLY_SER"     -> StorageLevel.MEMORY_ONLY_SER,
      "MEMORY_AND_DISK_SER" -> StorageLevel.MEMORY_AND_DISK_SER,
      "DISK_ONLY"           -> StorageLevel.DISK_ONLY,
      "MEMORY_ONLY_2"       -> StorageLevel.MEMORY_ONLY_2
    )
    levels.foreach { case (name, lvl) =>
      val r = buildRdd().persist(lvl)
      timed(s"$name - 1st action (fills cache)")(r.count())
      timed(s"$name - 2nd action (reads cache)")(r.count())
      r.unpersist(blocking = true)
    }

    println("\n=== TASK 4: caching hurts ===")
    timed("used ONCE, no cache")(buildRdd().count())
    timed("used ONCE, with cache (extra cost)")(buildRdd().cache().count())
    println("Cache tab kaam nahi aata jab RDD sirf 1 baar use ho, data memory se bada ho,")
    println("ya transformation sasta ho. Cache extra memory + GC + storage ka overhead deta hai.")

    println("\n=== TASK 5: scenario ===")
    val raw = sc.parallelize(Seq(
      "1,C001,Grocery,250.50", "2,C002,Electronics,1200.00", "3,C001,Grocery,-50",
      "bad line", "4,C003,Fashion,799.99", "5,C002,Grocery,120.00", "6,,Fashion,300",
      "7,C004,Electronics,45000.00", "8,C003,Fashion,abc", "9,C001,Electronics,999.00"
    ))

    def parse(line: String): Option[Txn] = {
      val p = line.split(",", -1).map(_.trim)
      if (p.length != 4) None
      else scala.util.Try(Txn(p(0).toInt, p(1), p(2), p(3).toDouble)).toOption
        .filter(t => t.amount > 0 && t.customer.nonEmpty)
    }

    val cleaned = raw.flatMap(parse).persist(StorageLevel.MEMORY_AND_DISK)

    println("Report 1: Revenue by category")
    cleaned.map(t => (t.category, t.amount)).reduceByKey(_ + _).collect().foreach(println)

    println("Report 2: Top 3 customers by spend")
    cleaned.map(t => (t.customer, t.amount)).reduceByKey(_ + _)
      .sortBy(-_._2).take(3).foreach(println)

    println("Report 3: Valid txn count & average amount")
    println(s"Valid: ${cleaned.count()}, Average: ${cleaned.map(_.amount).mean()}")

    cleaned.unpersist()
    spark.stop()
  }
}
