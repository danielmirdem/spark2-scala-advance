import org.apache.spark.{SparkConf, SparkContext}

object GetAllConfig {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("Get all config")

    val sc = new SparkContext(conf)

    sc.getConf.getAll.foreach(println)

  }
}
