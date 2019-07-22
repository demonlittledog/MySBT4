package car

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import temputer.JdbcHelper

object sparkScala {
  def main(args: Array[String]): Unit = {
//    val sql_str = "SELECT * FROM answers"
//    val r = JdbcHelper.query(sql_str)
//    r.foreach(println)
//    val conf = new SparkConf().setMaster("local[2]").setAppName("car_count")
//    val ssc = new StreamingContext(conf, Seconds(30))
//    val cars = ssc.socketTextStream("127.0.0.1",9999)
//    val pairs = cars.map(s =>(s.substring(0,8),1))
//    val car_count = pairs.reduceByKey(_ + _)
//    car_count.foreachRDD(x =>{
//      x.foreach(r =>{
//        val sql = "INSERT INTO car(carnum,cartime,camnum) VALUE ('"+r._2+"',now(),'"+r._1+"');"
//        JdbcHelper.update(sql)
//      })
//    })
//    car_count.print()
//    ssc.start()
//    //awaitTermination方法：接收人timeout和TimeUnit两个参数，用于设定超时时间及单位。当等待超过设定时间时，
//    // 会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。一般情况下会和shutdown方法组合使用
//    ssc.awaitTermination()


    val conf = new SparkConf().setMaster("local[*]").setAppName("Cars")
    val ssc = new StreamingContext(conf, Seconds(60))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.80.107:9092,192.168.80.108:9092,192.168.80.109:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "567",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("car3")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
        val pairs = stream.map(record => record.value).map(r => (r.substring(0, 8), 1))
        val car_count = pairs.reduceByKey(_ + _)
        car_count.foreachRDD(x =>{
          x.foreach(r =>{
            val sql = "INSERT INTO car(carnum,cartime,camnum) VALUE ('"+r._2+"',now(),'"+r._1+"');"
            JdbcHelper.update(sql)
          })
        })

    // 编写函数处理字符串"camera01:京M-3C9L2"，生成键值对

//    def split_value(v: String) = {
//      var cam_id = v.substring(0, 8)
//      var prov = v.substring(9, 10)
//      var car_num = v.substring(9)
//      ()
//    }

//stream.count().print()
//    // 对获取到的Kafka消息进行解析处理, 获取消息值字符串
//    stream.map(record => record.value).map(r => (r.substring(0, 8), r.substring(9))).print()
//    stream.map(record => record.value).map(r => (r.substring(0, 8), 1)).reduceByKey(_ + _).print()

    // 进行字符串拆分，得到camera id,省份，车牌等信息，进行统计分析
    // 以外地车牌占比为例：
    // 提取省份做key, 1为value, reduce by key, sort


    ssc.start()
    ssc.awaitTermination()
  }
}
