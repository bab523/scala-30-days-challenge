import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Try

object Day24StatelessVsStateful {

  // STATEFUL update function: purana state + current batch ki nayi values = naya state
  // state = (total_txn_count, total_amount)
  def updateFn(newValues: Seq[(Long, Double)],
               state: Option[(Long, Double)]): Option[(Long, Double)] = {
    val (oldCount, oldAmt) = state.getOrElse((0L, 0.0))
    Some((oldCount + newValues.map(_._1).sum, oldAmt + newValues.map(_._2).sum))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Day24StatelessVsStateful")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // Stateful operations ke liye checkpoint directory zaroori hai
    ssc.checkpoint("checkpoint")

    // Input format: ACCOUNT TYPE AMOUNT   (e.g. ACC101 DEPOSIT 5000)
    val lines = ssc.socketTextStream("localhost", 9999)

    // ---------- STATELESS transformations (har batch independent) ----------
    val txns = lines
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.split("\\s+"))
      .filter(_.length == 3)                                   // galat format hatao
      .map(p => (p(0), p(1).toUpperCase, Try(p(2).toDouble).getOrElse(-1.0)))
      .filter(_._3 > 0)                                        // invalid amount hatao

    // Current batch: account wise (count, amount)
    val currentBatch = txns
      .map { case (acc, _, amt) => (acc, (1L, amt)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    // ---------- STATEFUL transformation (batches ke beech state yaad rehta hai) ----------
    val runningState = currentBatch.updateStateByKey(updateFn _)

    // ---------- Output 1: stateless ----------
    currentBatch.foreachRDD { (rdd, time) =>
      println(s"\n[$time] STATELESS (current batch only):")
      val data = rdd.collect().sortBy(_._1)
      if (data.isEmpty) println("   (no transactions in this batch)")
      data.foreach { case (acc, (c, a)) => println(f"   $acc%-8s count=$c%d amount=$a%.2f") }
    }

    // ---------- Output 2: stateful ----------
    runningState.foreachRDD { (rdd, time) =>
      println(s"[$time] STATEFUL (running total since start):")
      rdd.collect().sortBy(_._1).foreach { case (acc, (c, a)) =>
        println(f"   $acc%-8s total_count=$c%d total_amount=$a%.2f")
      }
    }

    // ---------- Output 3: compare current batch vs accumulated ----------
    runningState.leftOuterJoin(currentBatch).foreachRDD { (rdd, time) =>
      println(s"[$time] COMPARE (batch vs accumulated):")
      rdd.collect().sortBy(_._1).foreach { case (acc, ((totalC, totalA), batch)) =>
        val batchC = batch.map(_._1).getOrElse(0L)
        println(f"   $acc%-8s this_batch=$batchC%d  accumulated=$totalC%d  total_amount=$totalA%.2f")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
