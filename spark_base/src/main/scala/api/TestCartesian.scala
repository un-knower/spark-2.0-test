package api

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 16/2/15.
 */
object TestCartesian extends App {


  val sparkConf = new SparkConf().setAppName("TestSecondSort")
    .setMaster("local[2]")
    .set("spark.cores.max", "1")
    .set("spark.driver.memory", "128m")
    .set("spark.local.dir", "/Users/yxl/data/spark.dir")

  val sc = new SparkContext(sparkConf)


  val rdd1 = sc.parallelize(List((1,Array(1,2,3,4)),(2,Array(2,3,4,5))))

  val rdd2 = sc.parallelize(List((1,Array(1,2,3,4)),(1,Array(2,2,3,4)),(3,Array(1,2,3,4)),(4,Array(2,3,4,5))))

  val rdd3 = rdd1.cartesian(rdd2)


  import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

  val rdd4 = rdd3.map({
    case ((f1,fl1),(f2,fl2)) => {
      ((f1,f2),fl1.apply(0) + fl2.apply(0))
    }
  })

  val rdd5 = rdd4.topByKey(1)(Ordering[Int])

   rdd5.foreach(x => println(x._1 + " " + x._2.mkString(",")))
}
