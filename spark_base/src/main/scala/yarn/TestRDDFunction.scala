package yarn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yxl on 15/11/17.
 */
object TestRDDFunction {

  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("TestFunction")
      .setMaster("local[*]")
      //.set("spark.cores.max", "2")
      //.set("spark.executor.memory", "128m")
      .set("spark.driver.memory", "128m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")

    val sc = new SparkContext(sparkConf);

    val list = List("a","b","c","d","e")

    val rdd1 = sc.parallelize((1 to 3).map(i => (i,list.take(i))))

    rdd1.foreach(println(_))

  }


}
