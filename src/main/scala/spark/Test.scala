package spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object Test {
  def main(args: Array[String]): Unit = {
    def avg_str(s: String): Boolean = {
      var num = s.split(",").map(_.toInt)
      var avg_val = num.sum / num.length
      avg_val > 22
    }
    println(avg_str("10,9,9,7,5,4,4,4,4,5,6,6,7,8,8,8,8,8,8,7,7,6,6,6"))
    val conf = new SparkConf().setMaster("local[*]").setAppName("Cars")
    val ssc = new StreamingContext(conf, Seconds(6))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.80.107:9092,192.168.80.108:9092,192.168.80.109:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "567",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("weather")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
  }
}
