import org.apache.spark.{SparkConf, SparkContext}
object Adj {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Usage: Adj <edge list path> <result path> <master>")
      System.exit(1)
    }
    val conf: SparkConf = new SparkConf().setAppName("ADJ").setMaster(args(2))
    val sc: SparkContext = new SparkContext(conf)

    val lines = sc.textFile(args(0))
    val doublelinks=lines.flatMap(l=>{
      val parts=l.split(("\t"))
      val edges:List[String]=List(l,parts(1)+"\t"+parts(0))
      edges
    })
    val lines2=doublelinks.map(l=>(l.split("\t")(0),l.split("\t")(1))).groupByKey().map(l=>{
      var x = new StringBuilder("")
      x.append(l._1)
      x.append(":"+l._1)
      var liste=l._2.toSet[String]
      for(s<-liste){
        x.append(","+s)
      }
      x
    })
    lines2.saveAsTextFile(args(1))
  }
}
