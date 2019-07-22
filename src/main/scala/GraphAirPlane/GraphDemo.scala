package GraphAirPlane

import org.apache.spark._
import org.apache.spark.graphx._
object GraphDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val cities = Array((1L,"哈尔滨"),(2L,"沈阳"),(3L,"北京"),(4L,"大连"))
    val airlines = Array(
      (3L,1L,("H01",1200,10000,100)),
      (3L,2L,("H02",600,8000,60)),
      (3L,4L,("H03",400,8000,40)),
      (2L,1L,("H04",600,8000,60)),
      (2L,3L,("H05",600,8000,60)),
      (4L,3L,("H06",400,8000,40)),
      (1L,4L,("H7",800,8000,80))
    )
    val cityRDD = sc.parallelize(cities)
    val airlineRDD = sc.parallelize(airlines).map(x => Edge(x._1,x._2,x._3))
    val myGraph = Graph(cityRDD,airlineRDD)
    myGraph.triplets.foreach(println)
    println("入度：")
    myGraph.inDegrees.foreach(println)
    println("出度：")
    myGraph.outDegrees.foreach(println)
    println("三角形：")
    myGraph.triangleCount().vertices.foreach(println)
//    println(airlineRDD)
//    println(airlineRDD.getClass)
//    println(airlineRDD.take(2))
  }
}
