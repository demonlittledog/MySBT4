import org.apache.spark.{SparkConf, SparkContext}
object MyScalaSparkDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //SparkContext 的初始化需要一个 SparkConf 对象， SparkConf 包含了Spark集群配置的各种参数（比如主节点的URL）
    conf.setAppName("Simple Application").setMaster("local[4]")
    //Spark 程序的编写都是从 SparkContext 开始的
    val sc = SparkContext.getOrCreate(conf)
    val data = Array(1,2,3,4,5,6,7,8,9,10)
    //将内存数据读入Spark系统中，作为一个整体数据集
    val rdd1 = sc.parallelize(data)
    var result = rdd1.map(x => x+5)
      .map(x => x*2)
      .map(x => x-10)
      .map(x => x/2)
        .collect()
    result.foreach(println)
    sc.stop()

  }
}
