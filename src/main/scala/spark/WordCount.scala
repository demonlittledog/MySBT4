package spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount  extends Serializable {
  def main(args: Array[String]): Unit = {
    //设置访问hdfs的用户名，Windows操作系统默认是user=Administrator，因此这里需要指定具体的写入用户。
    System.setProperty("HADOOP_USER_NAME", "bda")
    //声明配置
    val sparkConf = new SparkConf()
    //设置应用的名称
    sparkConf.setAppName("WordCount")
    //指定master，如果这里指定的话，在提交jar包时就可以不指定。
    sparkConf.setMaster("spark://192.168.80.109:7077")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * 以下代码就是业务逻辑
      */
    //读取hdfs的配置文件
    val  file = sc.textFile("hdfs://192.168.80.107:8020/data/README.md")
    //按照空格切割文件，得到每个单词
    val words = file.flatMap(_.split(" "))
    //将words里面的每一个单词都标记为1
    val wordOne = words.map((_,1))
    //聚合操作,将相同的key进行聚合
    val result = wordOne.reduceByKey(_+_)
    //将计算的结果写入到hdfs中
    result.saveAsTextFile("hdfs://192.168.80.107:8020/data/wordCount2")
    //关闭Spark链接
    sc.stop()
  }
}
