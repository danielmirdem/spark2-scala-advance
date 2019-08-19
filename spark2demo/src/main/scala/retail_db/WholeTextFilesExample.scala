package retail_db

import org.apache.spark.{SparkConf, SparkContext}

object WholeTextFilesExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Whole Text Files Example")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val orders = sc.wholeTextFiles("/home/danielmir/projects/udemy/emr-spark2-scala/dataset/data-master/retail_db/orders2/")

    println("Type: " + orders)


    println("First Key: "+ orders.first._1)

    println("First Value: ")
    orders.first._2.take(150).foreach(print)

    println
    println("Value Size: " + orders.first._2.size)


    println("Map -> Size of all contents------------")
    orders.map(_._2.size).collect.foreach(println)
  }
}
