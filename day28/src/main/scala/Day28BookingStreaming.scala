import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Try

// ---------- Schemas ----------
case class BookingEvent(bookingId: String, bookingType: String,
                        itemId: String, action: String, seats: Int)

case class BookingState(itemId: String, seats: Int, status: String)

object Day28BookingStreaming {

  def parse(line: String): Option[BookingEvent] = {
    val p = line.trim.split(",").map(_.trim)
    if (p.length != 5 || p(0).isEmpty) None
    else Try(BookingEvent(p(0), p(1).toUpperCase, p(2), p(3).toUpperCase, p(4).toInt)).toOption
      .filter(e => (e.action == "BOOK" || e.action == "CANCEL") && e.seats > 0)
  }

  // ---------- Stateful: har booking ka status yaad rakho ----------
  def updateFn(events: Seq[BookingEvent], state: Option[BookingState]): Option[BookingState] =
    events.foldLeft(state) { (st, e) =>
      e.action match {
        case "BOOK" =>
          if (st.isEmpty || st.get.status == "CANCELLED") Some(BookingState(e.itemId, e.seats, "BOOKED"))
          else st                                              // duplicate BOOK ignore
        case "CANCEL" =>
          st.filter(_.status == "BOOKED").map(_.copy(status = "CANCELLED")).orElse(st)
        case _ => st
      }
    }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day28BookingStreaming").setMaster("local[2]")

    val batchInterval = Seconds(5)
    val windowSize    = Seconds(30)
    val slideInterval = Seconds(10)
    val CANCEL_LIMIT  = 3   // window me itne cancellations = alert

    val ssc = new StreamingContext(conf, batchInterval)
    val sc  = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint")

    // ---------- Broadcast reference data: item -> (name, type, capacity) ----------
    val inventory: Map[String, (String, String, Int)] =
      sc.textFile("data/reference/inventory.csv")
        .filter(!_.startsWith("item_id"))
        .map(_.split(","))
        .map(a => (a(0).trim, (a(1).trim, a(2).trim, a(3).trim.toInt)))
        .collect().toMap
    val invBc = sc.broadcast(inventory)
    println(s"Inventory broadcast: ${invBc.value.size} items")

    // ---------- Read, parse, unknown item hatao (broadcast se check) ----------
    val lines  = ssc.socketTextStream("localhost", 9999)
    val events = lines
      .flatMap(l => parse(l).toList)
      .filter(e => invBc.value.contains(e.itemId))
      .cache()

    // ---------- Pair RDD: (itemId, (books, cancels)) window ----------
    val itemWindow = events
      .map(e => (e.itemId, (if (e.action == "BOOK") 1 else 0, if (e.action == "CANCEL") 1 else 0)))
      .reduceByKeyAndWindow((a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2),
                            windowSize, slideInterval)

    itemWindow.foreachRDD { (rdd, time) =>
      println(s"\n[$time] WINDOW (last ${windowSize.milliseconds / 1000}s) activity per item:")
      val data = rdd.collect().sortBy(_._1)
      if (data.isEmpty) println("   (no activity)")
      data.foreach { case (item, (b, c)) => println(f"   $item%-5s books=$b%d cancels=$c%d") }
      val totalCancels = data.map(_._2._2).sum
      if (totalCancels >= CANCEL_LIMIT)
        println(s"   *** ALERT: $totalCancels cancellations in window (limit $CANCEL_LIMIT) ***")
    }

    // ---------- Stateful booking state ----------
    val bookingState = events
      .map(e => (e.bookingId, e))
      .updateStateByKey[BookingState](updateFn _)

    // ---------- Spark SQL report ----------
    bookingState.foreachRDD { (rdd, time) =>
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      rdd.map { case (id, s) => (id, s.itemId, s.seats, s.status) }
        .toDF("booking_id", "item_id", "seats", "status")
        .createOrReplaceTempView("booking_state")

      invBc.value.toSeq
        .map { case (id, (name, tp, cap)) => (id, name, tp, cap) }
        .toDF("item_id", "item_name", "item_type", "capacity")
        .createOrReplaceTempView("inventory")

      val report = spark.sql("""
        SELECT item_id, item_name, item_type, capacity, occupied,
               capacity - occupied AS available,
               ROUND(occupied * 100.0 / capacity, 1) AS occupancy_pct,
               cancelled
        FROM (
          SELECT i.item_id, i.item_name, i.item_type, i.capacity,
                 COALESCE(SUM(CASE WHEN b.status = 'BOOKED' THEN b.seats END), 0) AS occupied,
                 COUNT(CASE WHEN b.status = 'CANCELLED' THEN 1 END)               AS cancelled
          FROM inventory i LEFT JOIN booking_state b ON i.item_id = b.item_id
          GROUP BY i.item_id, i.item_name, i.item_type, i.capacity
        ) t
        ORDER BY item_type, item_id
      """).cache()

      println(s"[$time] SQL REPORT: occupancy and availability per item")
      report.show(false)

      println("SQL REPORT: summary by type")
      spark.sql("""
        SELECT item_type, SUM(capacity) AS total_capacity, SUM(occupied) AS occupied,
               SUM(capacity) - SUM(occupied) AS available
        FROM (SELECT i.item_type, i.capacity,
                     COALESCE(SUM(CASE WHEN b.status='BOOKED' THEN b.seats END),0) AS occupied
              FROM inventory i LEFT JOIN booking_state b ON i.item_id = b.item_id
              GROUP BY i.item_id, i.item_type, i.capacity) x
        GROUP BY item_type ORDER BY item_type
      """).show(false)

      val over = report.filter($"occupied" > $"capacity")
      if (over.count() > 0) {
        println("*** OVERBOOKED ***")
        over.select("item_id", "item_name", "capacity", "occupied").show(false)
      }

      report.coalesce(1).write.mode("overwrite").option("header", "true").csv("output/booking_report")
      report.unpersist()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}


