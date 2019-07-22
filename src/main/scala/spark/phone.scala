package spark

import org.apache.spark.{SparkConf, SparkContext}

object phone {
  def main(args: Array[String]) {
    val logFile = "./src/main/scala/text.txt" // Should be some file on your server.
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
    val rdd1 = sc.textFile(logFile)
    val rdd2 = rdd1.flatMap(_.split("\n")).map((_,1)).reduceByKey(_ + _)
    rdd2.foreach(println)
    sc.stop()
  }

}