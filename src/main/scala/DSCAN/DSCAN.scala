import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, _}
import org.apache.spark.rdd.RDD

object DSCAN {
  type Vatt = (Int, List[Int], Int, String)

  def main(args: Array[String]): Unit = {
      if (args.length < 4) {
      println("Usage: DSCAN <Adj list path> <edge list path> <result path>")
      System.exit(1)
    }
    val t1 = System.nanoTime
   var adj=args(0)
   var  linksfile=args(1)
   var  rst=args(2)

 
      val conf: SparkConf = new SparkConf().setAppName("DSCAN").setMaster(args(3))
      val sc: SparkContext = new SparkContext(conf)

    val lines = sc.textFile(adj)
    val vertices: RDD[(Long, Vatt)] = lines.map(l => makeVertex(l))
    val links = sc.textFile(linksfile)
    val doublelinks=links.flatMap(l=>{
      val parts=l.split(("\t"))
     val edges:List[String]=List(l,parts(1)+"\t"+parts(0))
      edges
    })

    val edges: RDD[Edge[Double]] = doublelinks.map(l => Edge(l.split("\t")(0).toLong, l.split("\t")(1).toLong, 0))
    val mygraph = Graph(vertices, edges)


    val inval = List[Int]()
    // core detection step
    val CoreGraph = mygraph.pregel( 
      inval, 
      1, //	 maxIter: Int = Int.MaxValue,
      EdgeDirection.Out)(
      vprog1, //	 vprog: (VertexId, VD, A) => VD,
      send1, //	     sendMsg: EdgeTriplet[VD,ED] => Iterator[(VertexId, A)],
      mergeMsg1) //	 mergeMsg: (A, A) => A
    // border detection step
    val borderGraph = CoreGraph.pregel( 
      inval,
      1,
      EdgeDirection.Out)( 
      vprog2, 
      send2,
      mergeMsg2)
    // clusters building 
    val invalClusters = List[Any]()
    val clusters = borderGraph.pregel(
      invalClusters,
      1, 
      EdgeDirection.Out)(
      vprog3,
      send3,
      mergeMsg3)
      // Bridges and Outilies  identification step
    val finalrst = clusters.pregel( 
      invalClusters, 
      1, 
      EdgeDirection.Out)(
      vprog4, 
      send4, 
      mergeMsg4)
    // wrinting the final results
    val finalResults: RDD[String] = finalrst.vertices.map(l => (l._1 + ":" + l._2._3))
    finalResults.saveAsTextFile(rst)
    val duration = (System.nanoTime - t1) / 1e9d
    println("Running time is "+duration)
  }
  // functions definition
  def vprog1(vertexId: VertexId, features: Vatt, a: List[Int]): Vatt = {
    if (a.size == 0) {
      return (features._1, features._2, features._3, features._4)
    }
    else {
      val nb: Int = features._3 + a(0).toInt
      var statu: String = "n"
      if (nb >= 3) {
        statu = "core"
      }
      return (features._1, features._2, nb, statu)
    }
  }

  def send1(edgeTriplet: EdgeTriplet[(Int, List[Int], Int, String), Double]): Iterator[(Long, List[Int])] = {
    val sourceVertex = edgeTriplet.srcAttr
    var msg1: List[Int] = edgeTriplet.srcAttr._2
    var msg2: List[Int] = edgeTriplet.dstAttr._2
    val dis: Double = (msg1.intersect(msg2)).size.toDouble / math.sqrt(msg1.size * msg2.size).toDouble
    if (false)
     { 
      Iterator.empty // do nothing
    } else {
      if (dis >= 0.7) {
        Iterator((edgeTriplet.dstId, List[Int](1)))
      } else {
        Iterator((edgeTriplet.dstId, List[Int](0)))
      }

    }
  }

  def mergeMsg1(msg1: List[Int], msg2: List[Int]): List[Int] = {
    List[Int](msg1(0) + msg2(0))
  }

  // border step: pregel functions
  def vprog2(vertexId: VertexId, features: Vatt, a: List[Int]): Vatt = {
    if (a.size == 0) {
      return (features._1, features._2, features._3, features._4)
    }
    else {
      val nb: Int = features._3 + a(0).toInt
      var statu: String = "n"
      if (nb >= 1 && features._4 == "n") {
        statu = "border"
      }
      else if (features._4 == "core") {
        statu = "core"
      }
      return (features._1, features._2, features._3, statu)
    }
  }

  def send2(edgeTriplet: EdgeTriplet[(Int, List[Int], Int, String), Double]): Iterator[(Long, List[Int])] = {
    val sourceVertex = edgeTriplet.srcAttr
    var msg1: List[Int] = edgeTriplet.srcAttr._2
    var msg2: List[Int] = edgeTriplet.dstAttr._2
    val dis: Double = (msg1.intersect(msg2)).size.toDouble / math.sqrt(msg1.size * msg2.size).toDouble

      if (false)
      { 
      Iterator.empty // do nothing
    } else {
      if (dis >= 0.7 && (edgeTriplet.srcAttr._4 == "core" || edgeTriplet.dstAttr._4 == "core")) {
        Iterator((edgeTriplet.dstId, List[Int](1)))
      } else {
        Iterator((edgeTriplet.dstId, List[Int](0)))
      }

    }
  }

  def mergeMsg2(msg1: List[Int], msg2: List[Int]): List[Int] = {
    List[Int](msg1(0) + msg2(0))
  }

  // clustering step: pregel functions
  def send3(edgeTriplet: EdgeTriplet[(Int, List[Int], Int, String), Double]): Iterator[(Long, List[Any])] = {
    val sourceVertex = edgeTriplet.srcAttr
      if (false)
      {
      Iterator.empty // do nothing
    } else {
      Iterator((edgeTriplet.dstId, List[Any](sourceVertex._4, sourceVertex._1)))
    }
  }

  def mergeMsg3(msg1: List[Any], msg2: List[Any]): List[Any] = {
    if (msg1(0).toString == "core" && msg2(0).toString == "core") {
      return List[Any]("core", math.max(msg1(1).toString.toInt, msg2(1).toString.toInt))
    } else if (msg1(0).toString == "core" && msg2(0).toString != "core") {
      return List[Any]("core", msg1(1).toString.toInt)
    } else if (msg2(0).toString == "core" && msg1(0).toString != "core") {
      return List[Any]("core", msg2(1).toString.toInt)
    } else {
      return List[Any]("n", -1)
    }

  }
  def vprog3(vertexId: VertexId, features: Vatt, a: List[Any]): Vatt = {
    if (a.size == 0) {
      return (features._1, features._2, features._3, features._4)
    }
    else {
      if (a(0).toString == "core" && features._4 == "core") {
        (features._1, features._2, math.max(features._1, a(1).toString.toInt), features._4)
      } else if (a(0).toString == "core" && features._4 != "core") {
        (features._1, features._2, a(1).toString.toInt, features._4)
      }
      else {
        return (features._1, features._2, -1, features._4)
      }
    }
  }

  // other vertices
  def send4(edgeTriplet: EdgeTriplet[(Int, List[Int], Int, String), Double]): Iterator[(Long, List[Any])] = {
    val sourceVertex = edgeTriplet.srcAttr
      if (false)
      { 
      Iterator.empty // do nothing
    } else {
      Iterator((edgeTriplet.dstId, List[Any](sourceVertex._3)))
    }
  }

  def mergeMsg4(msg1: List[Any], msg2: List[Any]): List[Any] = {
    return List[Any](msg1(0).toString.toInt, msg2(0).toString.toInt)
  }

  def vprog4(vertexId: VertexId, features: Vatt, a: List[Any]): Vatt = {
    if (a.size == 0) {
      return (features._1, features._2, features._3, features._4)
    }
    else {
      if (features._3 == (-1)) {
        if (a.toSet.size > 1) {
          return (features._1, features._2, -2, features._4)
        } else {
          return (features._1, features._2, -1, features._4)
        }
      } else {
        return (features._1, features._2, features._3, features._4)
      }
    }
  }

  def makeVertex(line: String): (Long, Vatt) = {
    return (line.split(":")(0).toLong, (line.split(":")(0).toInt, line.split(":")(1).split(",").map(_.toInt).toList, 0, "n"))
  }
}
