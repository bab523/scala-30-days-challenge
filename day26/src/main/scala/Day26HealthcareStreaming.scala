import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Try

// ---------- Event schema ----------
case class VitalEvent(patientId: String,
                      heartRate: Int,
                      spo2: Int,
                      systolic: Int,
                      temperature: Double)

object Day26HealthcareStreaming {

  // Line ko VitalEvent me badlo, galat line ho to None
  def parse(line: String): Option[VitalEvent] = {
    val p = line.trim.split(",").map(_.trim)
    if (p.length != 5 || p(0).isEmpty) None
    else Try(VitalEvent(p(0), p(1).toInt, p(2).toInt, p(3).toInt, p(4).toDouble)).toOption
  }

  // Thresholds se compare karke abnormal vitals ki list do
  def checkAbnormal(e: VitalEvent, th: Map[String, (Double, Double)]): Seq[String] = {
    def out(name: String, v: Double): Boolean = {
      val (lo, hi) = th(name)
      v < lo || v > hi
    }
    Seq(
      if (out("heart_rate",  e.heartRate))    Some(s"HeartRate=${e.heartRate}")    else None,
      if (out("spo2",        e.spo2))         Some(s"SpO2=${e.spo2}")              else None,
      if (out("systolic_bp", e.systolic))     Some(s"BP=${e.systolic}")            else None,
      if (out("temperature", e.temperature))  Some(s"Temp=${e.temperature}")       else None
    ).flatten
  }

  // Stateful: purana running count + current batch ke counts
  def updateFn(newValues: Seq[Long], state: Option[Long]): Option[Long] =
    Some(state.getOrElse(0L) + newValues.sum)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Day26HealthcareStreaming")
      .setMaster("local[2]")

    val batchInterval = Seconds(5)
    val windowSize    = Seconds(30)
    val slideInterval = Seconds(10)
    val REPEAT_LIMIT  = 3L   // window me itni abnormal readings = CRITICAL

    val ssc = new StreamingContext(conf, batchInterval)
    val sc  = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpoint")

    // ---------- Broadcast: normal ranges (min, max) ----------
    val thresholds = sc.broadcast(Map(
      "heart_rate"  -> (50.0, 120.0),
      "spo2"        -> (92.0, 100.0),
      "systolic_bp" -> (90.0, 140.0),
      "temperature" -> (95.0, 100.4)
    ))

    // ---------- Accumulators ----------
    val totalAcc    = sc.longAccumulator("total_readings")
    val invalidAcc  = sc.longAccumulator("invalid_readings")
    val abnormalAcc = sc.longAccumulator("abnormal_readings")

    // ---------- Stream read + parse ----------
    val lines  = ssc.socketTextStream("localhost", 9999)
    val parsed = lines.map(parse).cache()   // cache: ek se zyada output me recompute na ho

    // Accumulators sirf yahan (ek hi jagah) update karte hain, taaki double count na ho
    parsed.foreachRDD { rdd =>
      rdd.foreach { o =>
        totalAcc.add(1)
        if (o.isEmpty) invalidAcc.add(1)
      }
    }

    val events = parsed.flatMap(_.toList)   // sirf valid events

    // ---------- Abnormal alerts (stateless) ----------
    val abnormal = events
      .map(e => (e, checkAbnormal(e, thresholds.value)))
      .filter(_._2.nonEmpty)
      .cache()

    abnormal.foreachRDD { (rdd, time) =>
      val alerts = rdd.collect()
      println(s"\n[$time] ---- Batch result ----")
      if (alerts.isEmpty) println("   no abnormal vitals in this batch")
      alerts.foreach { case (e, reasons) =>
        println(s"   ALERT patient=${e.patientId}  ${reasons.mkString(", ")}")
      }
      abnormalAcc.add(alerts.length.toLong)
      println(s"   [accumulators] total=${totalAcc.value} invalid=${invalidAcc.value} abnormal=${abnormalAcc.value}")
    }

    // ---------- Abnormal count per patient ----------
    val perPatient = abnormal.map { case (e, _) => (e.patientId, 1L) }.reduceByKey(_ + _)

    // ---------- Stateful: running abnormal count per patient ----------
    perPatient.updateStateByKey[Long](updateFn _).foreachRDD { (rdd, time) =>
      println(s"[$time] STATEFUL running abnormal count:")
      rdd.collect().sortBy(_._1).foreach { case (p, c) => println(s"   $p total_abnormal=$c") }
    }

    // ---------- Window: repeated abnormal readings ----------
    perPatient
      .reduceByKeyAndWindow((a: Long, b: Long) => a + b, windowSize, slideInterval)
      .foreachRDD { (rdd, time) =>
        val repeated = rdd.filter(_._2 >= REPEAT_LIMIT).collect()
        println(s"[$time] WINDOW (last ${windowSize.milliseconds / 1000}s) repeated abnormal check:")
        if (repeated.isEmpty) println("   no patient crossed the limit")
        repeated.foreach { case (p, c) =>
          println(s"   *** CRITICAL: patient $p had $c abnormal readings in window ***")
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }
}
