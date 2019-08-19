package retail_db

import org.apache.spark.{SparkConf, SparkContext}

object GetRevenueForOrderUsingParallelize {

  def main(args: Array[String]): Unit = {

    val orderItemsPath = args(1)
    val orderId = args(2).toInt

    val conf = new SparkConf().
      setMaster(args(0)).
      setAppName("Get revenue for orderId " + orderId +", using parallelize")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val orderItemsList = scala.io.Source.fromFile(orderItemsPath).
      getLines().toList

    val orderItems = sc.parallelize(orderItemsList)

    val orderItemsFiltered = orderItems.
      filter(e=> e.split(",")(1).toInt == orderId)

    val orderItemsSubtotals = orderItemsFiltered.
      map(e=> e.split(",")(4).toFloat)

    val orderRevenue = orderItemsSubtotals.reduce((curr, next) => curr + next)

    println("Order revenue for order id " + orderId + " is " + orderRevenue)

  }
}
