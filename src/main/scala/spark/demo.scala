package spark

import org.apache.spark.{SparkConf, SparkContext}

object demo {
  def main(args: Array[String]): Unit = {
    val a = Array("1 2 1 3 4 23 13 4 2 1 4 31 4 531 421 23 412")
    val conf = new SparkConf().setMaster("local[2]").setAppName("car_count")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val rdd1 = sc.parallelize(a)
    val rdd2 = rdd1.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_+_).filter(x => x._2>1)
    rdd2.foreach(println(_))
  }
}
