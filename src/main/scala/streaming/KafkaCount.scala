package streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaCount {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    var conf=new SparkConf().setMaster("local[*]")setAppName("SparkStream");
    var ssc=new StreamingContext(conf,Seconds(60));
    ssc.sparkContext.setLogLevel("WARN")
    //创建topic
    //var topic=Map{"test" -> 1}
    var topic=Array("streaming");
    //指定zookeeper
    //创建消费者组
    var group: String ="1"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> "192.168.80.109:9092,192.168.80.108:9092,192.168.80.107:9092",//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );
    //创建DStream，返回接收到的输入数据
    var stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
    //每一个stream都是一个ConsumerRecord
    //每分钟获取一个stream（DSTREAM是一个RDD)
    val pairs = stream.map(s =>s.value()).count().print()
    ssc.start()
    //awaitTermination方法：接收人timeout和TimeUnit两个参数，用于设定超时时间及单位。当等待超过设定时间时，
    // 会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。一般情况下会和shutdown方法组合使用
    ssc.awaitTermination()
  }
}
