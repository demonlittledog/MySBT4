package GraphAirPlane

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}


object airplane {
  def main(args: Array[String]): Unit = {
    val airlinesFile = "./src/main/scala/airlines.csv"
    val citiesFile = "./src/main/scala/cities.csv"
    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val airlines = sc.textFile(airlinesFile)
    val cities = sc.textFile(citiesFile)
    //引用数组使用x(0),使用元组使用x._1
    val cityRDD = cities.map(x => x.split(",")).map(x => (x(0).toLong,x(1)))
    val airlineRDD1 = airlines.map(x => x.split(",")).map(x => (x(0).toLong,x(1).toLong,(x(2).toLong,x(3).toLong)))
    val airlineRDD2 =airlineRDD1.map(x => Edge(x._1,x._2,x._3))
    val myGraph = Graph(cityRDD,airlineRDD2)
    val sourceId: VertexId = 5L
    //初始化一个图，用来表示起点值为0.0，其他点都为正无穷大∞
//    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
//    initialGraph.triplets.foreach(println)
    println("-"*60)
    myGraph.vertices.foreach(println)
    println("-"*60)
    myGraph.edges.foreach(println)
    println("-"*60)
    myGraph.triplets.foreach(println)
    println("-"*60)
    println("入度：")
    myGraph.inDegrees.foreach(println)
    println("出度：")
    myGraph.outDegrees.foreach(println)
    println("三角形：")
    myGraph.triangleCount().vertices.foreach(println)

}
}
