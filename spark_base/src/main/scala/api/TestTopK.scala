package api

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/9/14.
 */
object TestTopK {

  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("TestSecondSort")
    .setMaster("local[2]")
    .set("spark.cores.max", "1")
    .set("spark.driver.memory", "128m")
    .set("spark.local.dir", "/Users/yxl/data/spark.dir")
    val sc = new SparkContext(sparkConf);

    val line = sc.textFile("");
    val result = line.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)
    val sorted = result.map{case(key,value) => (value,key)}.sortByKey(true)
    val topk = sorted.top(10)

    topk.foreach(println(_))

  }

}
