package retail_db_list

object GetRevenueForOder {
  def main(args: Array[String]): Unit = {
    val orderItemsPath = args(0)
    val orderId = args(1).toInt

    val orderItems = scala.io.Source.fromFile(orderItemsPath).
      getLines().toList

    orderItems.take(10).foreach(println)
    println("---------------")

    val orderItemsFiltered = orderItems.
      filter(e=> e.split(",")(1).toInt == orderId)

    orderItemsFiltered foreach println

    println("--------------")

    val orderItemsSubtotals = orderItemsFiltered.
      map(e=> e.split(",")(4).toFloat)
    orderItemsSubtotals.foreach(println)

    println("--------------")

    val orderRevenue = orderItemsSubtotals.reduce((curr, next) => curr + next)

    println("Order revenue for order id " + orderId + " is " + orderRevenue)

  }
}
