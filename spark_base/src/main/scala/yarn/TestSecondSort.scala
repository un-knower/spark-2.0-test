package yarn

import org.apache.spark.{SparkContext, SparkConf}

import scala.math.Ordering

/**
 * Created by yxl on 15/8/27.
 */
object TestSecondSort {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestSecondSort")
      //.setMaster("local[2]")
      //.set("spark.cores.max", "1").set("spark.executor.memory", "128m")
      //.set("spark.driver.memory", "128m")
      //.set("spark.local.dir", "/Users/yxl/data/spark.dir")


    val sc = new SparkContext(sparkConf);

    val rdd = sc.textFile("hdfs://myhost:8020/data/secondsort.txt")

//    val finalRDD = rdd.map( x=> {val a = x.split(" ") ;  (a(0),a(1)) })
//    .groupByKey()
//    .sortByKey(true)
//    .map( x => (x._1,x._2.toList.sortWith((x1,x2) => x1 < x2)))
//    .flatMap(x => { x._2.map((x._1,_))})

    val finalRDD = rdd.map( x=> {val a = x.split(" ") ;  (a(0),a(1)) })
      .groupByKey()
      .sortByKey(true)
      .mapValues( x => x.toSeq.sorted(Ordering.String.reverse))
      .flatMap(x => { x._2.map((x._1,_))})


    import help.RDDExtend._

    val output = "hdfs://myhost:8020/resources/rdd/secondSort"

    finalRDD.saveAsMergedTextFile(output)

    sc.stop()
  }

}
