import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day11-Broadcast-App")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ---------- Task 1: Broadcast a small product reference map ----------
    println("---- Task 1: Broadcast a small product reference map ----")

    // Small "master" lookup table - product ID -> product name
    val productMasterData = Map(
      "P001" -> "Laptop",
      "P002" -> "Mouse",
      "P003" -> "Keyboard"
      // Note: P099, P100, P200 are intentionally missing (invalid products)
    )

    val broadcastProducts = sc.broadcast(productMasterData)
    println(s"Broadcasted product map: ${broadcastProducts.value}")
    println("This map is sent ONCE to each executor and cached there, instead of being")
    println("shipped again with every single task - saving network bandwidth.")

    // ---------- Task 2: Use an accumulator to count bad records ----------
    println("\n---- Task 2: Accumulator to count bad records ----")

    val badRecordsCounter = sc.longAccumulator("BadRecordsCounter")

    val txnRdd = sc.textFile("transactions.txt")

    val validatedRdd = txnRdd.map(line => {
      val parts = line.split(",")
      val txnId = parts(0)
      val productId = parts(1)
      val qty = parts(2).toInt

      val productName = broadcastProducts.value.get(productId)
      if (productName.isEmpty) {
        badRecordsCounter.add(1)  // count invalid/unknown product IDs
      }
      (txnId, productId, productName.getOrElse("UNKNOWN"), qty)
    })

    validatedRdd.collect() // action needed to actually trigger the accumulator updates
    println(s"Total bad records (unknown product IDs): ${badRecordsCounter.value}")

    // ---------- Task 3: Why normal driver variables shouldn't be used for distributed updates ----------
    println("\n---- Task 3: Why NOT to use normal driver variables ----")
    println("If we used a plain Scala variable (e.g. `var counter = 0`) instead of an")
    println("accumulator, and tried to increment it inside a `map` or `filter` closure,")
    println("Spark would SERIALIZE a COPY of that variable to EACH executor/task.")
    println("Each executor would increment its OWN local copy - the increments would")
    println("NEVER be sent back to the driver. The driver's original `counter` would")
    println("still show 0 after the job finishes, even though executors did the work.")
    println("Accumulators solve this: Spark knows to aggregate their updates from all")
    println("executors back to the driver safely, exactly once per task (for actions).")

    // ---------- Task 4: Combine broadcast data with RDD processing ----------
    println("\n---- Task 4: Combine broadcast data with RDD processing ----")

    println("Validated transactions (product name resolved via broadcast lookup):")
    validatedRdd.collect().foreach { case (txnId, pid, pname, qty) =>
      println(s"  $txnId -> Product: $pid ($pname), Qty: $qty")
    }

    // ---------- Task 5: Scenario - validate transactions against a small master table ----------
    println("\n---- Task 5: Scenario - Validate transactions against master table ----")

    val validTxns = validatedRdd.filter { case (_, _, pname, _) => pname != "UNKNOWN" }
    val invalidTxns = validatedRdd.filter { case (_, _, pname, _) => pname == "UNKNOWN" }

    println(s"Valid transactions: ${validTxns.count()}")
    validTxns.collect().foreach { case (txnId, pid, pname, qty) =>
      println(s"  VALID   -> $txnId, $pid ($pname), qty=$qty")
    }

    println(s"\nInvalid transactions (product not in master table): ${invalidTxns.count()}")
    invalidTxns.collect().foreach { case (txnId, pid, _, qty) =>
      println(s"  INVALID -> $txnId, $pid, qty=$qty")
    }

    println(s"\nFinal accumulator check - bad records counted: ${badRecordsCounter.value}")

    spark.stop()
  }
}
