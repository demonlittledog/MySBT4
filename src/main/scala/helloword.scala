import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Jan on 2016/12/19.
  */
object Helloworld {
//  def rdc(x:(String,Int),y:(String,Int)):(String,Int) ={
//    if(x._1.equals(y._1)){
//      (x._1,x._2+y._2)
//    }else{
//      ()
//    }
//  }
  def main(args: Array[String]) {
//    val logFile = "./src/main/scala/README.md" // Should be some file on your server.
//    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
//    val sc = SparkContext.getOrCreate(conf)
    //RDD.reduce
    // 求和求个数
//  val conf = new SparkConf()
//  conf.setAppName("Simple Application").setMaster("local[1]")
//  val sc = SparkContext.getOrCreate(conf)
//  val data = Array(99,22,57,18,33,29,45,71)
//  val sum = (x:(Int,Int),y:(Int,Int)) => (x._1+y._1,x._2+y._2)
//  val rdd1 = sc.parallelize(data)
//  val rdd2 = rdd1.map(x => (x,1))
//  val result = rdd2.reduce(sum)
//  println(result)

  // 求平均值
//  val conf = new SparkConf()
//  conf.setAppName("Simple Application").setMaster("local[1]")
//  val sc = SparkContext.getOrCreate(conf)
//  val data = Array(91.0,22,57,18,33,29,45,71)
//  val avg = (x:(Double,Int),y:(Double,Int)) => (((x._1*x._2+y._1*y._2)/(x._2+y._2):Double),x._2+y._2)
//  val rdd1 = sc.parallelize(data)
//  val rdd2 = rdd1.map((x:Double) => (x,1))
//  val result = rdd2.reduce(avg)
//  println(result)
//countByValue用法
//  val conf = new SparkConf()
//  conf.setAppName("Simple Application").setMaster("local[1]")
//  val sc = SparkContext.getOrCreate(conf)
//  val data = Array(91.0,22,57,18,33,29,45,71,33)
//  val rdd1 = sc.parallelize(data)
//  val result = rdd1.countByValue()
//  println(result)
  // 读取文件中的内容
//  val logFile = "./src/main/scala/README.md" // Should be some file on your server.
//  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
//  val sc = SparkContext.getOrCreate(conf)
//  val rdd1 = sc.textFile(logFile)
//  val rdd2 = rdd1.flatMap(x =>x.split(" "))
//  val result = rdd2.countByValue()
//  val array = result.toArray
//  val rdd3 = sc.parallelize(array)
//  val rdd4 = rdd3.filter(x => x._2>2)
//  println(rdd3.take(1))
//  val collect = rdd4.collect()
//  collect.foreach(print)

  val logFile = "./src/main/scala/README.md" // Should be some file on your server.
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = SparkContext.getOrCreate(conf)
  val rdd1 = sc.textFile(logFile)
  val rdd2 = rdd1.flatMap(n =>n.split(" ")).map(n => (n,1,0))
  println(rdd2.first())
  val rdd3 = rdd2.groupBy(x => x._1)
//    val rdd1 = sc.textFile(logFile)
//    val rdd2 = rdd1.flatMap(x => x.split(" "))
//    val rdd3 = rdd2.map(x => (x,1))
//    val rdd4 = rdd3.reduce(rdc _)
//    println(rdd4)
    sc.stop()
  }

}