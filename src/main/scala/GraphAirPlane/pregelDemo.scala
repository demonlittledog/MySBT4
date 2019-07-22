package GraphAirPlane

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}


object pregelDemo {
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
  val airlineRDD1 = airlines.map(x => x.split(",")).map(x => (x(0).toLong,x(1).toLong,x(2).toLong))
  val airlineRDD2 =airlineRDD1.map(x => Edge(x._1,x._2,x._3))
  val graph = Graph(cityRDD,airlineRDD2)
  val sourceId: VertexId = 4L
  //初始化一个图，用来表示起点值为0.0，其他点都为正无穷大∞
  val initialGraph = graph.mapVertices((id, _) =>
    if (id == sourceId) 0.0 else Double.PositiveInfinity)
  val sssp = initialGraph.pregel(
    Double.PositiveInfinity,
    activeDirection = EdgeDirection.Out
  )(
    //1, vprog,作用是处理到达顶点的参数，取较小的那个作为顶点的值
    (vertexId, vertexValue, msg) =>
      math.min(vertexValue, msg),
    //2, sendMsg,计算权重，如果邻居节点的属性加上边上的距离小于该节点的属性，说明从源节点比从邻居节点到该顶点的距离更小，更新值
    triplet => {
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    },
    //3, mergeMsg，合并到达顶点的所有信息
    (a, b) => math.min(a, b)
  )
  println(sssp.vertices.collect.mkString("\n"))

  }
}
