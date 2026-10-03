import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Try

object Day25WindowOperations {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day25WindowOperations")
      .setMaster("local[2]")

    // ---------- Intervals ----------
    // Window size aur slide, dono batch interval ke multiple hone chahiye
    val batchInterval = Seconds(5)
    val windowSize    = Seconds(30)   // task me: Minutes(10)
    val slideInterval = Seconds(10)   // task me: Minutes(1)

    // Spike rule: window count >= MIN_SPIKE aur pichli window se >= 2 guna
    val MIN_SPIKE = 8L
    val GROWTH    = 2.0

    val ssc = new StreamingContext(conf, batchInterval)
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint")

    // Input format: TXN_ID STORE AMOUNT   (e.g. T1 STORE_A 500)
    val lines = ssc.socketTextStream("localhost", 9999)

    val txns = lines
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.split("\\s+"))
      .filter(_.length == 3)
      .map(p => (p(0), p(1), Try(p(2).toDouble).getOrElse(-1.0)))
      .filter(_._3 > 0)

    // ---------- 1. countByWindow: window me kul transactions ----------
    val windowCount = txns.countByWindow(windowSize, slideInterval)

    // ---------- 2. reduceByKeyAndWindow: store wise rolling sales total ----------
    val storeSales = txns
      .map { case (_, store, amt) => (store, amt) }
      .reduceByKeyAndWindow((a: Double, b: Double) => a + b, windowSize, slideInterval)

    // ---------- 3. reduceByWindow: overall rolling sales total ----------
    val totalSales = txns
      .map(_._3)
      .reduceByWindow(_ + _, windowSize, slideInterval)

    // ---------- Output ----------
    var lastCount = 0L   // pichli window ka count (driver par chalta hai)

    windowCount.foreachRDD { (rdd, time) =>
      val count = rdd.collect().headOption.getOrElse(0L)
      println(s"\n[$time] WINDOW (last ${windowSize.milliseconds / 1000}s) transaction count = $count (previous window = $lastCount)")

      val isSpike = count >= MIN_SPIKE && lastCount > 0 && count >= GROWTH * lastCount
      val isSpikeFromZero = count >= MIN_SPIKE && lastCount == 0
      if (isSpike || isSpikeFromZero)
        println(s"   >>> ALERT: sudden increase in transactions! $lastCount -> $count")
      else
        println("   status: normal")

      lastCount = count
    }

    storeSales.foreachRDD { (rdd, time) =>
      println(s"[$time] Rolling sales by store:")
      val data = rdd.collect().sortBy(_._1)
      if (data.isEmpty) println("   (no sales in this window)")
      data.foreach { case (s, a) => println(f"   $s%-10s total=$a%.2f") }
    }

    totalSales.foreachRDD { (rdd, time) =>
      val total = rdd.collect().headOption.getOrElse(0.0)
      println(f"[$time] Rolling total sales (all stores) = $total%.2f")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
