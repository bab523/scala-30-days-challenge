import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreamsBasics {
  def main(args: Array[String]): Unit = {

    // local[2]: 1 thread receiver ke liye, 1 processing ke liye (minimum 2 zaroori)
    val conf = new SparkConf()
      .setAppName("Day23DStreamsBasics")
      .setMaster("local[2]")

    // Batch interval = 5 seconds => har 5 sec ka data ek micro-batch
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    // ---------- Socket text stream ----------
    // Log format: 2025-01-05 10:15:01 ERROR Database connection failed
    val lines = ssc.socketTextStream("localhost", 9999)

    println("Streaming started... localhost:9999 se logs aa rahe hain")

    // ---------- map: har line ko trim karo ----------
    val cleaned = lines.map(_.trim).filter(_.nonEmpty)

    // ---------- flatMap: line ko words me todo ----------
    val words = cleaned.flatMap(_.split("\\s+"))

    // ---------- filter: sirf ERROR lines ----------
    val errorLines = cleaned.filter(_.split("\\s+").lift(2).contains("ERROR"))

    // ---------- Scenario: ERROR messages ka count har interval me ----------
    val errorCount = errorLines.count()

    // ---------- Log level wise count (map + reduceByKey) ----------
    val levelCounts = cleaned
      .map(_.split("\\s+"))
      .filter(_.length >= 3)
      .map(parts => (parts(2), 1))
      .reduceByKey(_ + _)

    // ---------- Output (har batch pe chalta hai) ----------
    errorCount.foreachRDD { (rdd, time) =>
      println(s"\n[$time] ERROR count in this batch: ${rdd.collect().headOption.getOrElse(0L)}")
    }

    errorLines.foreachRDD { (rdd, time) =>
      val errs = rdd.collect()
      if (errs.nonEmpty) {
        println(s"[$time] ERROR lines:")
        errs.foreach(e => println(s"   $e"))
      }
    }

    levelCounts.foreachRDD { (rdd, time) =>
      println(s"[$time] Level wise count: " + rdd.collect().sortBy(_._1).mkString(", "))
    }

    words.map(w => (w, 1)).reduceByKey(_ + _).foreachRDD { (rdd, time) =>
      val top = rdd.collect().sortBy(-_._2).take(3)
      println(s"[$time] Top 3 words: " + top.mkString(", "))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
