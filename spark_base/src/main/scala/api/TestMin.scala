package api

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/10/27.
 */
object TestMin {

  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("TestSecondSort")
      .setMaster("local[2]")
      .set("spark.cores.max", "1")
      .set("spark.driver.memory", "128m")
      .set("spark.local.dir", "/Users/yxl/data/spark.dir")
    val sc = new SparkContext(sparkConf);


    val baseRDD = sc.textFile("/Users/yxl/data/sortKey.txt")

    val mapRDD = baseRDD.map(x=> {
      val array = x.split(",")
      (array(0),array(2))
    }).reduceByKey((x1,x2) => {
        if(x1.toInt > x2.toInt){
             x1
        }else{
            x2
        }
    }).foreach(println(_))



  }



}
