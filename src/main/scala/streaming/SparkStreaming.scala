package streaming

import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext, _}
object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("car_count")
    val ssc = new StreamingContext(conf, Seconds(10))
    val cars = ssc.socketTextStream("127.0.0.1",9999)
    //每分钟获取一个cars（DSTREAM是一个RDD)
    // "京B-1A2B3"
    val pairs = cars.map(s =>(s.substring(0,1),1))
    val car_count = pairs.reduceByKey(_ + _)
    car_count.print()
    ssc.start()
    //awaitTermination方法：接收人timeout和TimeUnit两个参数，用于设定超时时间及单位。当等待超过设定时间时，
    // 会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。一般情况下会和shutdown方法组合使用
    ssc.awaitTermination()
  }
}
