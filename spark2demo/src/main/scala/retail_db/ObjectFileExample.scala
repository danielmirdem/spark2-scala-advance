package retail_db

import org.apache.spark.{SparkConf, SparkContext}

object ObjectFileExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Object File Example")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val orderItems = sc.textFile("/home/danielmir/projects/udemy/emr-spark2-scala/dataset/data-master/retail_db/orders");

    orderItems.saveAsObjectFile("/home/danielmir/projects/udemy/emr-spark2-scala/dataset/data-master/retail_db/orders_object_file")
  }
}
