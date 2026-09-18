import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day6-WordCount-App")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    val logsRdd = sc.textFile("applogs.txt")

    // ---------- Task 1: Implement classic Word Count ----------
    println("---- Task 1: Classic Word Count ----")

    val wordCounts = logsRdd
      .flatMap(line => line.split("\\s+"))   // split lines into words
      .map(word => (word, 1))                 // pair each word with 1
      .reduceByKey(_ + _)                     // sum counts per word

    wordCounts.collect().take(10).foreach { case (word, count) =>
      println(s"$word -> $count")
    }

    // ---------- Task 2: Explain flatMap -> map -> reduceByKey ----------
    println("\n---- Task 2: Explain flatMap -> map -> reduceByKey ----")
    println("flatMap: splits each line into individual words and flattens all words")
    println("         from every line into a single RDD (many lines -> one flat list of words).")
    println("map: transforms each word into a (word, 1) tuple, preparing it for counting.")
    println("reduceByKey: groups tuples by the word (key) and sums up the 1s (values),")
    println("             giving the total occurrence count for each word.")

    // ---------- Task 3: Case-insensitive counts ----------
    println("\n---- Task 3: Case-insensitive Word Count ----")

    val caseInsensitiveCounts = logsRdd
      .flatMap(line => line.split("\\s+"))
      .map(word => word.toLowerCase)          // normalize case
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    caseInsensitiveCounts.collect().take(10).foreach { case (word, count) =>
      println(s"$word -> $count")
    }

    // ---------- Task 4: Ignore punctuation and empty words ----------
    println("\n---- Task 4: Clean Word Count (no punctuation/empty words) ----")

    val cleanCounts = logsRdd
      .flatMap(line => line.split("\\s+"))
      .map(word => word.toLowerCase.replaceAll("[^a-z0-9]", "")) // strip punctuation
      .filter(word => word.nonEmpty)          // remove empty strings
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    cleanCounts.collect().foreach { case (word, count) =>
      println(s"$word -> $count")
    }

    // ---------- Task 5: Scenario - top 10 most frequent words ----------
    println("\n---- Task 5: Top 10 Most Frequent Words ----")

    val top10 = cleanCounts
      .sortBy({ case (_, count) => count }, ascending = false)
      .take(10)

    top10.zipWithIndex.foreach { case ((word, count), idx) =>
      println(s"${idx + 1}. $word -> $count")
    }

    spark.stop()
  }
}
