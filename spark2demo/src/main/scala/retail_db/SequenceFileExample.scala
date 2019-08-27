package retail_db

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

object SequenceFileExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Sequence File Example")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val orderItems = sc.textFile("/home/danielmir/projects/udemy/emr-spark2-scala/dataset/data-master/retail_db/orders");

    //orderItems.take(10).foreach(println)

    orderItems.map(e=> (NullWritable.get, e)).saveAsSequenceFile("/home/danielmir/projects/udemy/emr-spark2-scala/dataset/data-master/retail_db/orders_seq")

    val orderItemsSeq = sc.sequenceFile("/home/danielmir/projects/udemy/emr-spark2-scala/dataset/data-master/retail_db/orders_seq", classOf[NullWritable], classOf[Text])

    orderItemsSeq.
      map(r=> r._2.toString).take(10).foreach(println)


  }

}
