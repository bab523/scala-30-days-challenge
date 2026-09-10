object Main {

  // Case classes for Scenario
  case class Customer(id: Int, name: String, city: String)
  case class Order(customerId: Int, product: String, qty: Int, price: Double)

  def main(args: Array[String]): Unit = {

    // ---------- Task 1: map, filter, flatMap, reduce on sales List ----------
    println("---- Task 1: Sales List operations ----")
    val sales: List[Double] = List(1200.0, 450.5, 3000.0, 0.0, 899.99)

    val withTax = sales.map(s => s * 1.18) // map: apply tax
    println(s"Sales with tax: $withTax")

    val validSales = sales.filter(_ > 0) // filter: remove zero sales
    println(s"Valid sales (non-zero): $validSales")

    val nested = List(List(1.0, 2.0), List(3.0, 4.0))
    val flat = nested.flatMap(x => x) // flatMap: flatten nested list
    println(s"Flattened list: $flat")

    val total = sales.reduce((a, b) => a + b) // reduce: sum all
    println(s"Total sales: $total")

    // ---------- Task 2: Vector for indexed customer records ----------
    println("\n---- Task 2: Vector for customers ----")
    val customers: Vector[Customer] = Vector(
      Customer(1, "Amit", "Delhi"),
      Customer(2, "Priya", "Mumbai"),
      Customer(3, "Kiran", "Pune")
    )
    // Vector gives fast O(1) random/indexed access, unlike List which is O(n)
    println(s"Customer at index 1: ${customers(1)}")
    println("Vector is useful here because we often need quick lookup by index, " +
      "and Vector provides efficient random access compared to List's linear traversal.")

    // ---------- Task 3: Map for product quantities and prices ----------
    println("\n---- Task 3: Map for products ----")
    val productQty: Map[String, Int] = Map(
      "Laptop" -> 5,
      "Mouse" -> 20,
      "Keyboard" -> 15
    )
    val productPrice: Map[String, Double] = Map(
      "Laptop" -> 55000.0,
      "Mouse" -> 500.0,
      "Keyboard" -> 1200.0
    )

    productQty.foreach { case (product, qty) =>
      val price = productPrice.getOrElse(product, 0.0)
      println(s"$product -> Qty: $qty, Price: $price, Total: ${qty * price}")
    }

    // ---------- Task 4: for-comprehension combining customers and orders ----------
    println("\n---- Task 4: Combine customers and orders ----")
    val orders: List[Order] = List(
      Order(1, "Laptop", 1, 55000.0),
      Order(2, "Mouse", 2, 500.0),
      Order(3, "Keyboard", 1, 1200.0)
    )

    val combined = for {
      order <- orders
      customer <- customers if customer.id == order.customerId
    } yield s"${customer.name} (${customer.city}) ordered ${order.qty} x ${order.product}"

    combined.foreach(println)

    // ---------- Task 5: Scenario - daily sales summary without Spark ----------
    println("\n---- Task 5: Daily Sales Summary ----")
    val dailySummary = orders.map(o => o.qty * o.price).sum
    println(s"Total orders today: ${orders.size}")
    println(s"Total revenue today: $dailySummary")

    val topOrder = orders.maxBy(o => o.qty * o.price)
    println(s"Highest value order: ${topOrder.product} worth ${topOrder.qty * topOrder.price}")

    val productWiseRevenue = orders.groupBy(_.product).map {
      case (product, ordersList) => product -> ordersList.map(o => o.qty * o.price).sum
    }
    println(s"Product-wise revenue: $productWiseRevenue")
  }
}
