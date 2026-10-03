import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Try

// ---------- Transaction event schema ----------
case class TxnEvent(txnId: String,
                    accountId: String,
                    branchId: String,
                    txnType: String,
                    amount: Double)

object Day27BankingStreaming {

  def parse(line: String): Option[TxnEvent] = {
    val p = line.trim.split(",").map(_.trim)
    if (p.length != 5 || p(0).isEmpty || p(1).isEmpty) None
    else Try(TxnEvent(p(0), p(1), p(2), p(3).toUpperCase, p(4).toDouble))
      .toOption.filter(_.amount > 0)
  }

  // Stateful: (count, amount) ka running total per account
  def updateFn(newValues: Seq[(Long, Double)],
               state: Option[(Long, Double)]): Option[(Long, Double)] = {
    val (c, a) = state.getOrElse((0L, 0.0))
    Some((c + newValues.map(_._1).sum, a + newValues.map(_._2).sum))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Day27BankingStreaming")
      .setMaster("local[2]")      // YARN par chalate waqt ye line hata do

    val batchInterval = Seconds(5)
    val windowSize    = Seconds(30)
    val slideInterval = Seconds(10)
    val BURST_LIMIT   = 5L        // window me itni txns = suspicious burst
    val BIG_AMOUNT    = 50000.0   // high-risk branch par itna amount = alert

    val ssc = new StreamingContext(conf, batchInterval)
    val sc  = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint")

    // ---------- Reference data: chhoti table, ek baar load, partition + persist ----------
    val partitioner = new HashPartitioner(2)
    val branchRef = sc.textFile("data/reference/branch_risk.csv")
      .filter(!_.startsWith("branch_id"))
      .map(_.split(","))
      .map(a => (a(0).trim, (a(1).trim, a(2).trim, a(3).trim)))   // (branchId, (name, city, risk))
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_ONLY)
    println(s"Reference branches loaded: ${branchRef.count()}")

    // ---------- Stream read + parse ----------
    val lines  = ssc.socketTextStream("localhost", 9999)
    val events = lines.flatMap(l => parse(l).toList).cache()   // 3-4 jagah use hota hai

    // ---------- 1. Aggregate by account (current batch) ----------
    val perAccount = events
      .map(e => (e.accountId, (1L, e.amount)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    perAccount.foreachRDD { (rdd, time) =>
      println(s"\n[$time] ---- Batch: account wise ----")
      val data = rdd.collect().sortBy(_._1)
      if (data.isEmpty) println("   (no transactions)")
      data.foreach { case (acc, (c, a)) => println(f"   $acc%-8s txns=$c%d amount=$a%.2f") }
    }

    // ---------- 2. Stateful: running totals ----------
    perAccount.updateStateByKey[(Long, Double)](updateFn _).foreachRDD { (rdd, time) =>
      println(s"[$time] RUNNING totals since start:")
      rdd.collect().sortBy(_._1).foreach { case (acc, (c, a)) =>
        println(f"   $acc%-8s total_txns=$c%d total_amount=$a%.2f")
      }
    }

    // ---------- 3. Join with branch/risk reference data ----------
    val enriched = events
      .map(e => (e.branchId, e))
      .transform { rdd =>
        // stream RDD ko same partitioner do, taaki persisted reference RDD shuffle na ho
        rdd.partitionBy(partitioner).leftOuterJoin(branchRef)
      }
      .map { case (_, (e, ref)) =>
        val (name, city, risk) = ref.getOrElse(("UNKNOWN", "UNKNOWN", "UNKNOWN"))
        (e, name, city, risk)
      }
      .cache()

    enriched.foreachRDD { (rdd, time) =>
      val rows = rdd.collect()
      println(s"[$time] ENRICHED transactions:")
      rows.foreach { case (e, name, city, risk) =>
        val flag =
          if (risk == "UNKNOWN") "  <-- UNKNOWN BRANCH"
          else if (risk == "HIGH" && e.amount >= BIG_AMOUNT) "  <-- ALERT: big amount at HIGH risk branch"
          else ""
        println(f"   ${e.txnId}%-5s ${e.accountId}%-7s $name%-17s risk=$risk%-8s ${e.amount}%.2f$flag")
      }
    }

    // ---------- 4. Suspicious burst: window me txn count per account ----------
    events
      .map(e => (e.accountId, (1L, e.amount)))
      .reduceByKeyAndWindow((a: (Long, Double), b: (Long, Double)) => (a._1 + b._1, a._2 + b._2),
                            windowSize, slideInterval)
      .foreachRDD { (rdd, time) =>
        println(s"[$time] WINDOW (last ${windowSize.milliseconds / 1000}s) burst check:")
        val bursts = rdd.filter(_._2._1 >= BURST_LIMIT).collect()
        if (bursts.isEmpty) println("   no suspicious burst")
        bursts.foreach { case (acc, (c, a)) =>
          println(f"   *** SUSPICIOUS BURST: $acc made $c%d txns, total $a%.2f in window ***")
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}
