package api

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/9/14.
 */
object TestAverage {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestAverage")
    .setMaster("local[2]")
    .set("spark.cores.max", "1")
    .set("spark.driver.memory", "128m")
    .set("spark.local.dir", "/Users/yxl/data/spark.dir")
    val sc = new SparkContext(sparkConf)


    val data = sc.parallelize(Seq(("A", 2), ("A", 4), ("B", 2), ("Z", 0), ("B", 10)))

    val mapValuesRDD = data.mapValues((_, 1))

    mapValuesRDD.foreach(println)

    val reduceRDD = mapValuesRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    def  fun (x:(Int,Int)):Double = {
        val (sum,count) = x
        return (1.0 * sum)/count
    }

    val sumRDD = reduceRDD.mapValues(fun) // mapValues 里面传递的是函数

    val resultMap = sumRDD.collectAsMap()

    resultMap.foreach(println(_))



  }


}
